import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type IssueParamsBase = { owner: string; repo: string; issue_number: number };

type CheckRunLikeBase = {
  id?: number | null;
  status?: string | null;
  conclusion?: string | null;
  head_sha?: string | null;
  html_url?: string | null;
};

type CheckSuiteLikeBase = {
  id?: number | null;
  status?: string | null;
  conclusion?: string | null;
  head_sha?: string | null;
  head_branch?: string | null;
};

type RegistryValidationArtifactsBase<MachineReadableSourceType> = {
  byFile: Map<string, string[]>;
  machineReadableSources: MachineReadableSourceType[];
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export type CheckCompletedHandlerCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  RegistryValidationArtifactsType extends RegistryValidationArtifactsBase<MachineReadableSourceType>,
  MachineReadableSourceType,
> = {
  readCheckRunFromPayload: (payload: unknown) => CheckRunType | null;
  readCheckSuiteFromPayload: (payload: unknown) => CheckSuiteType | null;
  readRepoInfoFromPayload: (payload: unknown) => RepoInfoType | null;
  readCheckRunPrNumbers: (run: CheckRunType | null) => number[];
  resolveCheckSuitePrNumbers: (
    context: ContextType,
    repoInfo: RepoInfoType,
    suite: CheckSuiteType | null,
    headSha: string
  ) => Promise<number[]>;
  readCheckSuiteId: (suite: CheckSuiteType | null) => number | null;
  listAllCheckRunsForSuite: (
    context: ContextType,
    owner: string,
    repo: string,
    checkSuiteId: number
  ) => Promise<CheckRunType[]>;
  readCheckRunId: (run: CheckRunType | null) => number | null;
  readFirstRegistryValidationArtifactsForSuiteRuns: (
    context: ContextType,
    owner: string,
    repo: string,
    runsForSuite: CheckRunType[]
  ) => Promise<RegistryValidationArtifactsType | null>;
  readPullRequestHtmlUrl: (context: ContextType, repoInfo: RepoInfoType, prNumber: number) => Promise<string>;
  collapseBotCommentsByPrefix: (
    context: ContextType,
    params: IssueParamsBase,
    options: {
      tagPrefix: string;
      keepTags?: string[];
      collapseBody?: string;
      classifier?: 'OUTDATED' | 'RESOLVED' | 'DUPLICATE' | 'OFF_TOPIC' | 'SPAM' | 'ABUSE';
    }
  ) => Promise<void>;
  postCheckSuiteRegistryValidationComments: (
    context: ContextType,
    repoInfo: RepoInfoBase,
    prNumbers: number[],
    artifacts: RegistryValidationArtifactsType,
    minimizeTag: string
  ) => Promise<void>;
  maybeHandleDefaultBranchCheckSuiteSuccess: (
    context: ContextType,
    payload: unknown,
    checkSuite: CheckSuiteType | null,
    repoInfo: RepoInfoBase
  ) => Promise<void>;
  tryAutoMerge: (context: ContextType, repoInfo: RepoInfoBase, headSha: string) => Promise<void>;
  maybeApprovePendingWorkflowRunsForPrNumbers: (
    context: ContextType,
    repoInfo: RepoInfoBase,
    prNumbers: number[],
    headSha: string,
    reason: string
  ) => Promise<boolean>;
  handleBlockingRegistryHeadConclusion: (
    context: ContextType,
    repoInfo: RepoInfoBase,
    headSha: string,
    baseBranch: string,
    reason: string
  ) => Promise<boolean>;
  isBlockingCheckConclusion: (conclusion: string) => boolean;
  readDefaultBranchFromPayload: (payload: unknown) => string;
  getStaticConfig: (context: ContextType) => Promise<unknown>;
  log: (context: ContextType, level: 'debug' | 'info' | 'warn' | 'error', obj: unknown, msg: string) => void;
  isDebugEnabled: boolean;
};

type CheckCompletedCallbackSet<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  RegistryValidationArtifactsType extends RegistryValidationArtifactsBase<MachineReadableSourceType>,
  MachineReadableSourceType,
> = CheckCompletedHandlerCallbacks<
  ContextType,
  RepoInfoType,
  CheckRunType,
  CheckSuiteType,
  RegistryValidationArtifactsType,
  MachineReadableSourceType
>;

function logCheckCompletedEventClassification<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  RegistryValidationArtifactsType extends RegistryValidationArtifactsBase<MachineReadableSourceType>,
  MachineReadableSourceType,
>(
  context: ContextType,
  eventName: string,
  action: string,
  run: CheckRunType | null,
  checkSuite: CheckSuiteType | null,
  repoInfo: RepoInfoType | null,
  callbacks: CheckCompletedCallbackSet<
    ContextType,
    RepoInfoType,
    CheckRunType,
    CheckSuiteType,
    RegistryValidationArtifactsType,
    MachineReadableSourceType
  >
): void {
  callbacks.log(
    context,
    'info',
    {
      event: eventName,
      action,
      hasCheckRun: Boolean(run),
      hasCheckSuite: Boolean(checkSuite),
      checkRunHeadSha: toStringTrim(run?.head_sha),
      checkRunStatus: toStringTrim(run?.status).toLowerCase(),
      checkRunConclusion: toStringTrim(run?.conclusion).toLowerCase(),
      checkSuiteHeadSha: toStringTrim(checkSuite?.head_sha),
      checkSuiteHeadBranch: toStringTrim(checkSuite?.head_branch),
      checkSuiteStatus: toStringTrim(checkSuite?.status).toLowerCase(),
      checkSuiteConclusion: toStringTrim(checkSuite?.conclusion).toLowerCase(),
      owner: repoInfo?.owner,
      repo: repoInfo?.repo,
    },
    'checks:event-classification'
  );
}

async function collapseResolvedCiValidationComments<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  RegistryValidationArtifactsType extends RegistryValidationArtifactsBase<MachineReadableSourceType>,
  MachineReadableSourceType,
>(
  context: ContextType,
  repoInfo: RepoInfoBase,
  prNumbers: number[],
  callbacks: CheckCompletedCallbackSet<
    ContextType,
    RepoInfoType,
    CheckRunType,
    CheckSuiteType,
    RegistryValidationArtifactsType,
    MachineReadableSourceType
  >
): Promise<void> {
  for (const prNumber of prNumbers) {
    await callbacks.collapseBotCommentsByPrefix(
      context,
      { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: prNumber },
      {
        tagPrefix: 'nsreq:ci-validation',
        collapseBody: 'Validation issues resolved.',
        classifier: 'RESOLVED',
      }
    );
  }
}

async function maybeHandleBlockingCompletedCheck<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  RegistryValidationArtifactsType extends RegistryValidationArtifactsBase<MachineReadableSourceType>,
  MachineReadableSourceType,
>(
  context: ContextType,
  payload: unknown,
  repoInfo: RepoInfoBase,
  prNumbers: number[],
  headShaStr: string,
  conclusion: string,
  reasonPrefix: 'check-run' | 'check-suite',
  callbacks: CheckCompletedCallbackSet<
    ContextType,
    RepoInfoType,
    CheckRunType,
    CheckSuiteType,
    RegistryValidationArtifactsType,
    MachineReadableSourceType
  >
): Promise<boolean> {
  if (!callbacks.isBlockingCheckConclusion(conclusion)) return false;

  await callbacks.getStaticConfig(context);

  if (conclusion === 'action_required') {
    const approvedWorkflow = await callbacks.maybeApprovePendingWorkflowRunsForPrNumbers(
      context,
      repoInfo,
      prNumbers,
      headShaStr,
      `${reasonPrefix}:${conclusion}`
    );

    if (approvedWorkflow) return true;
  }

  await callbacks.handleBlockingRegistryHeadConclusion(
    context,
    repoInfo,
    headShaStr,
    callbacks.readDefaultBranchFromPayload(payload),
    `${reasonPrefix}:${conclusion}`
  );

  return false;
}

async function readPullRequestFilesUrlsForSuite<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  RegistryValidationArtifactsType extends RegistryValidationArtifactsBase<MachineReadableSourceType>,
  MachineReadableSourceType,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  prNumbers: number[],
  callbacks: CheckCompletedCallbackSet<
    ContextType,
    RepoInfoType,
    CheckRunType,
    CheckSuiteType,
    RegistryValidationArtifactsType,
    MachineReadableSourceType
  >
): Promise<void> {
  const prFilesUrlByNumber = new Map<number, string>();

  for (const prNumber of prNumbers) {
    try {
      const html = await callbacks.readPullRequestHtmlUrl(context, repoInfo, prNumber);
      if (html) prFilesUrlByNumber.set(prNumber, `${html}/files`);
    } catch {
      // keep behavior: ignore URL lookup failures
    }
  }
}

async function postCheckSuiteRegistryValidationCommentsIfPresent<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  RegistryValidationArtifactsType extends RegistryValidationArtifactsBase<MachineReadableSourceType>,
  MachineReadableSourceType,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  checkSuite: CheckSuiteType,
  prNumbers: number[],
  callbacks: CheckCompletedCallbackSet<
    ContextType,
    RepoInfoType,
    CheckRunType,
    CheckSuiteType,
    RegistryValidationArtifactsType,
    MachineReadableSourceType
  >
): Promise<void> {
  const suiteId = callbacks.readCheckSuiteId(checkSuite);
  if (!suiteId) return;
  if (!prNumbers.length) return;

  if (callbacks.isDebugEnabled) {
    callbacks.log(context, 'debug', { suiteId, prNumbers }, 'dbg:checks:failure suite');
  }

  let runsForSuite: CheckRunType[];
  try {
    runsForSuite = await callbacks.listAllCheckRunsForSuite(context, repoInfo.owner, repoInfo.repo, suiteId);

    if (callbacks.isDebugEnabled) {
      callbacks.log(
        context,
        'debug',
        {
          suiteId,
          runsForSuite: runsForSuite.map((r) => ({
            id: callbacks.readCheckRunId(r),
            conclusion: toStringTrim(r.conclusion),
            url: toStringTrim(r.html_url),
          })),
        },
        'dbg:checks:runs listed for suite'
      );
    }
  } catch {
    return;
  }

  await readPullRequestFilesUrlsForSuite(context, repoInfo, prNumbers, callbacks);

  const registryValidationArtifacts = await callbacks.readFirstRegistryValidationArtifactsForSuiteRuns(
    context,
    repoInfo.owner,
    repoInfo.repo,
    runsForSuite
  );

  if (!registryValidationArtifacts) return;

  await callbacks.postCheckSuiteRegistryValidationComments(
    context,
    repoInfo,
    prNumbers,
    registryValidationArtifacts,
    'nsreq:ci-validation'
  );
}

async function handleCompletedCheckRun<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  RegistryValidationArtifactsType extends RegistryValidationArtifactsBase<MachineReadableSourceType>,
  MachineReadableSourceType,
>(
  context: ContextType,
  payload: unknown,
  eventName: string,
  action: string,
  run: CheckRunType,
  repoInfo: RepoInfoType | null,
  callbacks: CheckCompletedCallbackSet<
    ContextType,
    RepoInfoType,
    CheckRunType,
    CheckSuiteType,
    RegistryValidationArtifactsType,
    MachineReadableSourceType
  >
): Promise<void> {
  const conclusion = toStringTrim(run?.conclusion).toLowerCase();
  const status = toStringTrim(run?.status).toLowerCase();
  const headShaStr = toStringTrim(run?.head_sha);

  if (!repoInfo) {
    callbacks.log(
      context,
      'warn',
      {
        event: eventName,
        action,
        conclusion,
        status,
        headShaStr,
      },
      'checks:check-run-missing-repo-info'
    );
    return;
  }

  const prNumbers = callbacks.readCheckRunPrNumbers(run);

  callbacks.log(
    context,
    'info',
    {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      conclusion,
      status,
      headShaStr,
      prNumbers,
    },
    'checks:check-run resolved'
  );

  if (status !== 'completed') return;

  if (conclusion && conclusion !== 'success') {
    await maybeHandleBlockingCompletedCheck(
      context,
      payload,
      repoInfo,
      prNumbers,
      headShaStr,
      conclusion,
      'check-run',
      callbacks
    );
    return;
  }

  if (conclusion !== 'success') return;
  if (!headShaStr) return;

  if (prNumbers.length === 0) {
    callbacks.log(
      context,
      'info',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        headShaStr,
      },
      'checks:check-run-deferred-no-pr-mapping'
    );
    return;
  }

  await collapseResolvedCiValidationComments(context, repoInfo, prNumbers, callbacks);
  await callbacks.tryAutoMerge(context, repoInfo, headShaStr);
}

async function handleCompletedCheckSuite<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  RegistryValidationArtifactsType extends RegistryValidationArtifactsBase<MachineReadableSourceType>,
  MachineReadableSourceType,
>(
  context: ContextType,
  payload: unknown,
  checkSuite: CheckSuiteType,
  repoInfo: RepoInfoType,
  callbacks: CheckCompletedCallbackSet<
    ContextType,
    RepoInfoType,
    CheckRunType,
    CheckSuiteType,
    RegistryValidationArtifactsType,
    MachineReadableSourceType
  >
): Promise<void> {
  const conclusion = toStringTrim(checkSuite.conclusion).toLowerCase();
  const headShaStr = toStringTrim(checkSuite.head_sha);
  const ownerLogin = repoInfo.owner;
  const repoName = repoInfo.repo;

  const prNumbers = await callbacks.resolveCheckSuitePrNumbers(context, repoInfo, checkSuite, headShaStr);

  callbacks.log(
    context,
    'info',
    {
      ownerLogin,
      repoName,
      conclusion,
      headShaStr,
      checkSuiteHeadBranch: toStringTrim(checkSuite.head_branch),
      checkSuiteStatus: toStringTrim(checkSuite.status).toLowerCase(),
      prNumbers,
    },
    'checks:context resolved'
  );

  if (conclusion === 'success') {
    await collapseResolvedCiValidationComments(context, repoInfo, prNumbers, callbacks);

    await callbacks.maybeHandleDefaultBranchCheckSuiteSuccess(context, payload, checkSuite, {
      owner: ownerLogin,
      repo: repoName,
    });

    if (!headShaStr) return;

    await callbacks.tryAutoMerge(context, { owner: ownerLogin, repo: repoName }, headShaStr);
    return;
  }

  if (callbacks.isBlockingCheckConclusion(conclusion)) {
    const approvedWorkflow = await maybeHandleBlockingCompletedCheck(
      context,
      payload,
      { owner: ownerLogin, repo: repoName },
      prNumbers,
      headShaStr,
      conclusion,
      'check-suite',
      callbacks
    );

    if (approvedWorkflow) return;
  }

  await postCheckSuiteRegistryValidationCommentsIfPresent(context, repoInfo, checkSuite, prNumbers, callbacks);
}

export async function handleCheckCompletedEvent<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  RegistryValidationArtifactsType extends RegistryValidationArtifactsBase<MachineReadableSourceType>,
  MachineReadableSourceType,
>(
  context: ContextType,
  payload: unknown,
  eventName: string,
  callbacks: CheckCompletedHandlerCallbacks<
    ContextType,
    RepoInfoType,
    CheckRunType,
    CheckSuiteType,
    RegistryValidationArtifactsType,
    MachineReadableSourceType
  >
): Promise<void> {
  const action = isPlainObject(payload) ? toStringTrim(payload['action']).toLowerCase() : '';
  const run = callbacks.readCheckRunFromPayload(payload);
  const checkSuite = callbacks.readCheckSuiteFromPayload(payload);
  const repoInfo = callbacks.readRepoInfoFromPayload(payload);

  logCheckCompletedEventClassification(context, eventName, action, run, checkSuite, repoInfo, callbacks);

  if (run) {
    await handleCompletedCheckRun(context, payload, eventName, action, run, repoInfo, callbacks);
    return;
  }

  if (!checkSuite) return;
  if (!repoInfo) return;

  await handleCompletedCheckSuite(context, payload, checkSuite, repoInfo, callbacks);
}
