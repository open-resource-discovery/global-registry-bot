import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type IssueParamsBase = { owner: string; repo: string; issue_number: number };

type IssueLikeBase = {
  number: number;
  body?: string | null;
};

type TemplateLikeBase = {
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type PullRequestBaseLike = {
  ref?: string | null;
};

type PullRequestLikeBase = {
  number: number;
  body?: string | null;
  base?: PullRequestBaseLike;
};

type ApprovalHandlingResult = 'approved' | 'rejected' | 'continue';

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export type PullRequestAutoMergeEntryCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  PullRequestType extends PullRequestLikeBase,
> = {
  isSequentialDirectRegistryPr: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<boolean>;
  shouldDeferSequentialDirectRegistryPrProcessing: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType
  ) => Promise<boolean>;
  parseLinkedIssueNumberFromPr: (pr: PullRequestType, repoInfo?: RepoInfoType) => number | null;
  readFreshPullRequest: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number
  ) => Promise<PullRequestType | null>;
  maybeHandleStandaloneDirectPrApproval: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    options?: { baseBranch?: string }
  ) => Promise<ApprovalHandlingResult>;
  tryMergeApprovedPrOrUpdateBranch: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<void>;
  buildIssueParams: (repoInfo: RepoInfoType, issueNumber: number) => ParamsType;
  readLinkedIssue: (context: ContextType, params: ParamsType) => Promise<IssueType>;
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
  getErrorMessage: (error: unknown) => string;
  getHttpStatus: (error: unknown) => number | undefined;
  isCrossRepositoryPullRequest: (pr: PullRequestType, repoInfo: RepoInfoType) => boolean;
  hasIssueFormInputs: (issue: IssueType) => boolean;
  loadTemplateWithLabelRefresh: (context: ContextType, params: ParamsType, issue: IssueType) => Promise<TemplateType>;
  parseForm: (body: string, template: TemplateType) => FormDataType;
  readIssueBodyForProcessing: (body: unknown) => string;
  isRequestIssue: (context: ContextType, template: TemplateType, parsedFormData: FormDataType) => boolean;
  buildCompatibleRequestSnapshotHashes: (
    issueBody: unknown,
    parsedFormData: FormDataType,
    template: TemplateType
  ) => string[];
  calcSnapshotHash: (parsedFormData: FormDataType, template: TemplateType, rawBody: string) => string;
  extractHashFromPrBody: (body: string) => string;
  closeOutdatedRequestPrs: (
    context: ContextType,
    params: ParamsType,
    template: TemplateType,
    options: { parsedFormData?: FormDataType; currentHash?: string; acceptedHashes?: string[] }
  ) => Promise<void>;
  maybeHandleDirectPrApprovalForMerge: (
    context: ContextType,
    repoInfo: RepoInfoType,
    issueParams: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    pr: PullRequestType
  ) => Promise<ApprovalHandlingResult>;
};

export async function processPullRequestForAutoMerge<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  PullRequestType extends PullRequestLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  callbacks: PullRequestAutoMergeEntryCallbacks<
    ContextType,
    RepoInfoType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    PullRequestType
  >
): Promise<void> {
  const prBaseBranch = toStringTrim(pr.base?.ref);

  if (await callbacks.isSequentialDirectRegistryPr(context, repoInfo, pr, prBaseBranch)) {
    if (await callbacks.shouldDeferSequentialDirectRegistryPrProcessing(context, repoInfo, pr)) {
      return;
    }
  }

  const issueNumber = callbacks.parseLinkedIssueNumberFromPr(pr, repoInfo);

  if (issueNumber === null) {
    const freshPr = (await callbacks.readFreshPullRequest(context, repoInfo, pr.number)) || pr;
    const standaloneOutcome = await callbacks.maybeHandleStandaloneDirectPrApproval(context, repoInfo, freshPr, {
      baseBranch: toStringTrim(freshPr.base?.ref),
    });

    if (standaloneOutcome !== 'approved') return;

    const approvedPr = (await callbacks.readFreshPullRequest(context, repoInfo, freshPr.number)) || freshPr;
    await callbacks.tryMergeApprovedPrOrUpdateBranch(context, repoInfo, approvedPr, 'auto-merge');
    return;
  }

  const params = callbacks.buildIssueParams(repoInfo, issueNumber);

  let issue: IssueType;
  try {
    issue = await callbacks.readLinkedIssue(context, params);
  } catch (error: unknown) {
    callbacks.log(
      context,
      'warn',
      {
        prNumber: pr.number,
        issueNumber,
        err: callbacks.getErrorMessage(error),
        status: callbacks.getHttpStatus(error),
        crossRepo: callbacks.isCrossRepositoryPullRequest(pr, repoInfo),
      },
      'direct-pr:linked-issue-read-failed-fallback-standalone'
    );

    const freshPr = (await callbacks.readFreshPullRequest(context, repoInfo, pr.number)) || pr;
    const standaloneOutcome = await callbacks.maybeHandleStandaloneDirectPrApproval(context, repoInfo, freshPr, {
      baseBranch: toStringTrim(freshPr.base?.ref),
    });

    if (standaloneOutcome !== 'approved') return;

    const approvedPr = (await callbacks.readFreshPullRequest(context, repoInfo, freshPr.number)) || freshPr;
    await callbacks.tryMergeApprovedPrOrUpdateBranch(context, repoInfo, approvedPr, 'auto-merge');
    return;
  }

  if (!process.env.JEST_WORKER_ID && !callbacks.hasIssueFormInputs(issue)) {
    callbacks.log(
      context,
      'info',
      {
        prNumber: pr.number,
        issueNumber,
      },
      'direct-pr:linked-issue-not-request-form-fallback-standalone'
    );

    const standaloneOutcome = await callbacks.maybeHandleStandaloneDirectPrApproval(context, repoInfo, pr);
    if (standaloneOutcome !== 'approved') return;

    await callbacks.tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
    return;
  }

  let template: TemplateType;
  try {
    template = await callbacks.loadTemplateWithLabelRefresh(context, params, issue);
  } catch (error: unknown) {
    callbacks.log(
      context,
      'warn',
      {
        prNumber: pr.number,
        issueNumber,
        err: callbacks.getErrorMessage(error),
        status: callbacks.getHttpStatus(error),
      },
      'direct-pr:linked-issue-template-load-failed-fallback-standalone'
    );

    const standaloneOutcome = await callbacks.maybeHandleStandaloneDirectPrApproval(context, repoInfo, pr);
    if (standaloneOutcome !== 'approved') return;

    await callbacks.tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
    return;
  }

  const parsedFormData = template
    ? callbacks.parseForm(callbacks.readIssueBodyForProcessing(issue.body), template)
    : {};
  if (!callbacks.isRequestIssue(context, template, parsedFormData as FormDataType)) {
    callbacks.log(
      context,
      'info',
      {
        prNumber: pr.number,
        issueNumber,
        parsedKeys: Object.keys(parsedFormData || {}),
      },
      'direct-pr:linked-issue-not-request-issue-fallback-standalone'
    );

    const standaloneOutcome = await callbacks.maybeHandleStandaloneDirectPrApproval(context, repoInfo, pr);
    if (standaloneOutcome !== 'approved') return;

    await callbacks.tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
    return;
  }

  const body = toStringTrim(pr.body);
  const snapshotHashes = callbacks.buildCompatibleRequestSnapshotHashes(
    issue.body,
    parsedFormData as FormDataType,
    template
  );
  const currentHash =
    snapshotHashes[0] ||
    callbacks.calcSnapshotHash(
      parsedFormData as FormDataType,
      template,
      callbacks.readIssueBodyForProcessing(issue.body)
    );
  const prHash = callbacks.extractHashFromPrBody(body);

  if (prHash) {
    if (!snapshotHashes.includes(prHash)) {
      await callbacks.closeOutdatedRequestPrs(context, params, template, {
        parsedFormData: parsedFormData as FormDataType,
        currentHash,
        acceptedHashes: snapshotHashes,
      });
      return;
    }

    await callbacks.tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
    return;
  }

  const directPrOutcome = await callbacks.maybeHandleDirectPrApprovalForMerge(
    context,
    repoInfo,
    params,
    issue,
    template,
    parsedFormData as FormDataType,
    pr
  );

  if (directPrOutcome !== 'approved') return;

  await callbacks.tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
}
