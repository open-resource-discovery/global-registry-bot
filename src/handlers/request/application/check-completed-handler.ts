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

  if (run) {
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
      if (callbacks.isBlockingCheckConclusion(conclusion)) {
        await callbacks.getStaticConfig(context);

        await callbacks.handleBlockingRegistryHeadConclusion(
          context,
          repoInfo,
          headShaStr,
          callbacks.readDefaultBranchFromPayload(payload),
          `check-run:${conclusion}`
        );
      }

      return;
    }
    if (conclusion !== 'success') return;
    if (!headShaStr) return;

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

    await callbacks.tryAutoMerge(context, repoInfo, headShaStr);
    return;
  }

  if (!checkSuite) return;
  if (!repoInfo) return;

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
    for (const prNumber of prNumbers) {
      await callbacks.collapseBotCommentsByPrefix(
        context,
        { owner: ownerLogin, repo: repoName, issue_number: prNumber },
        {
          tagPrefix: 'nsreq:ci-validation',
          collapseBody: 'Validation issues resolved.',
          classifier: 'RESOLVED',
        }
      );
    }

    await callbacks.maybeHandleDefaultBranchCheckSuiteSuccess(context, payload, checkSuite, {
      owner: ownerLogin,
      repo: repoName,
    });

    if (!headShaStr) return;
    await callbacks.tryAutoMerge(context, { owner: ownerLogin, repo: repoName }, headShaStr);
    return;
  }

  if (callbacks.isBlockingCheckConclusion(conclusion)) {
    await callbacks.getStaticConfig(context);

    await callbacks.handleBlockingRegistryHeadConclusion(
      context,
      { owner: ownerLogin, repo: repoName },
      headShaStr,
      callbacks.readDefaultBranchFromPayload(payload),
      `check-suite:${conclusion}`
    );
  }

  const suiteId = callbacks.readCheckSuiteId(checkSuite);
  if (!suiteId) return;
  if (!prNumbers.length) return;

  if (callbacks.isDebugEnabled) {
    callbacks.log(context, 'debug', { suiteId, prNumbers }, 'dbg:checks:failure suite');
  }

  let runsForSuite: CheckRunType[] = [];
  try {
    runsForSuite = await callbacks.listAllCheckRunsForSuite(context, ownerLogin, repoName, suiteId);
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

  const prFilesUrlByNumber = new Map<number, string>();
  for (const prNumber of prNumbers) {
    try {
      const html = await callbacks.readPullRequestHtmlUrl(context, repoInfo, prNumber);
      if (html) prFilesUrlByNumber.set(prNumber, `${html}/files`);
    } catch {
      // ignore
    }
  }

  const registryValidationArtifacts = await callbacks.readFirstRegistryValidationArtifactsForSuiteRuns(
    context,
    ownerLogin,
    repoName,
    runsForSuite
  );
  if (!registryValidationArtifacts) return;

  await callbacks.postCheckSuiteRegistryValidationComments(
    context,
    { owner: ownerLogin, repo: repoName },
    prNumbers,
    registryValidationArtifacts,
    'nsreq:ci-validation'
  );
}
