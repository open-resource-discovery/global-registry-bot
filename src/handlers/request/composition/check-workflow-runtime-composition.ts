import { type ApprovalDecision } from '../domain/approval-decision.js';
import { type RegistryValidationMachineReadableSource } from '../domain/registry-validation-annotations.js';
import {
  maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication,
  maybeApprovePendingWorkflowRunsForPrNumbersApplication,
  type WorkflowApprovalCallbacks,
} from '../application/workflow-approval.js';
import {
  updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry as updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetryApplication,
  type DefaultBranchApprovedPrBranchUpdateCallbacks,
} from '../application/default-branch-approved-pr-branch-update.js';
import {
  reevaluateOpenDirectPullRequestsAfterDefaultBranchPush as reevaluateOpenDirectPullRequestsAfterDefaultBranchPushApplication,
  type DefaultBranchDirectPrReevaluationCallbacks,
} from '../application/default-branch-direct-pr-reevaluation.js';
import {
  maybeHandleDefaultBranchCheckSuiteSuccess as maybeHandleDefaultBranchCheckSuiteSuccessApplication,
  type DefaultBranchCheckSuiteReevaluationCallbacks,
} from '../application/default-branch-check-suite-reevaluation.js';
import { type CheckCompletedHandlerCallbacks } from '../application/check-completed-handler.js';
import { composeCheckCompletedHandlerCallbacks } from './checks-composition.js';
import {
  composeDefaultBranchApprovedPrBranchUpdateCallbacks,
  composeDefaultBranchDirectPrReevaluationCallbacks,
} from './default-branch-push-composition.js';
import { composeDefaultBranchCheckSuiteReevaluationCallbacks } from './default-branch-check-suite-composition.js';
import { composeWorkflowApprovalCallbacks } from './workflow-approval-composition.js';

const DEFAULT_BRANCH_UPDATE_RETRY_DELAY_MS = 5000;

type RuntimeLogLevel = 'debug' | 'info' | 'warn' | 'error';

type ResourceBotContextBase = {
  resourceBotHooksSource?: string | null;
  octokit: {
    rest: {
      repos: {
        getBranch: (args: { owner: string; repo: string; branch: string }) => Promise<{
          data?: {
            commit?: {
              sha?: string | null;
            };
          };
        }>;
      };
      pulls: {
        get: (params: { owner: string; repo: string; pull_number: number }) => Promise<{ data: unknown }>;
      };
    };
  };
};

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type PullRequestBranchLikeBase = {
  ref?: string | null;
  sha?: string | null;
};

type PullRequestLikeBase = {
  number: number;
  title?: string | null;
  body?: string | null;
  state?: string | null;
  user?: unknown | null;
  head: PullRequestBranchLikeBase;
  base?: PullRequestBranchLikeBase;
  mergeable?: boolean | null;
  mergeable_state?: string | null;
  draft?: boolean | null;
};

type PullRequestFileLikeBase = {
  filename?: string | null;
  status?: string | null;
};

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

type SequentialRegistryPrResultBase = {
  updated: boolean;
  processed: boolean;
  blockedByActive: boolean;
};

type DirectPrApprovalOptionsBase = {
  baseBranch?: string;
};

export type CheckWorkflowRuntimeDependencies<
  ContextType extends ResourceBotContextBase,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  PullRequestFileType extends PullRequestFileLikeBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  SequentialRegistryPrResultType extends SequentialRegistryPrResultBase,
> = {
  log: (context: ContextType | undefined, level: RuntimeLogLevel, obj: unknown, msg: string) => void;
  getErrorMessage: (error: unknown) => string;
  getHttpStatus: (error: unknown) => number | undefined;
  toStringTrim: (value: unknown) => string;
  isDebugEnabled: boolean;

  getStaticConfig: (context: ContextType, options?: { forceReload?: boolean }) => Promise<unknown>;

  isPullRequestOpen: (pr: PullRequestType | null | undefined) => boolean;
  isSafeRegistryWorkflowApprovalFile: (context: ContextType, file: PullRequestFileType) => boolean;
  listChangedFilesForPr: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number
  ) => Promise<PullRequestFileType[]>;
  parseLinkedIssueNumberFromPr: (pr: PullRequestType, repoInfo?: RepoInfoType) => number | null;
  isSnapshotManagedRequestPr: (pr: PullRequestType) => boolean;
  evaluateDirectPrOnApproval: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    requestAuthorIdOverride?: string,
    options?: DirectPrApprovalOptionsBase
  ) => Promise<ApprovalDecision>;
  hasAllowedStandaloneDirectPrApprovalForCurrentHead: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    decision: ApprovalDecision,
    options?: DirectPrApprovalOptionsBase
  ) => Promise<boolean>;
  readFreshPullRequest: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number
  ) => Promise<PullRequestType | null>;
  isPlainObject: (value: unknown) => value is Record<string, unknown>;

  isSequentialRegistryPrActiveBlocking: (context: ContextType, repoInfo: RepoInfoType) => Promise<boolean>;
  listOpenPullRequests: (context: ContextType, repoInfo: RepoInfoType) => Promise<PullRequestType[]>;
  isSequentialRegistryPrHeadSkipped: (repoInfo: RepoInfoType, pr: PullRequestType) => boolean;
  listChangedYamlFilesForPrWithFallback: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<string[]>;
  isPullRequestApprovedForBranchMaintenance: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    options?: { allowLabelFallback?: boolean }
  ) => Promise<boolean>;
  waitForPullRequestMergeability: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<PullRequestType>;
  isPullRequestDirty: (pr: PullRequestType | null | undefined) => boolean;
  readMergeableState: (pr: PullRequestType | null | undefined) => string;
  shouldUpdatePullRequestBranch: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch: string
  ) => Promise<boolean>;
  requestPullRequestBranchUpdate: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<boolean>;
  markSequentialRegistryPrHeadSkipped: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => void;

  readDefaultBranchFromPayload: (payload: unknown) => string;

  runOneSequentialDirectRegistryPrMaintenance: (
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string,
    reason: string
  ) => Promise<SequentialRegistryPrResultType>;

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
  ) => Promise<{
    byFile: Map<string, string[]>;
    machineReadableSources: RegistryValidationMachineReadableSource[];
  } | null>;
  collapseBotCommentsByPrefix: (
    context: ContextType,
    params: { owner: string; repo: string; issue_number: number },
    options: {
      perPage?: number;
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
    artifacts: {
      byFile: Map<string, string[]>;
      machineReadableSources: RegistryValidationMachineReadableSource[];
    },
    minimizeTag: string
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
};

type CheckWorkflowRuntime<
  ContextType extends ResourceBotContextBase,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  PullRequestFileType extends PullRequestFileLikeBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  SequentialRegistryPrResultType extends SequentialRegistryPrResultBase,
> = {
  buildWorkflowApprovalCallbacks: () => WorkflowApprovalCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    PullRequestFileType
  >;
  maybeApprovePendingWorkflowRunsForRegistryPrWithRetry: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<boolean>;
  maybeApprovePendingWorkflowRunsForPrNumbers: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumbers: number[],
    headSha: string,
    reason: string
  ) => Promise<boolean>;

  buildDefaultBranchApprovedPrBranchUpdateCallbacks: () => DefaultBranchApprovedPrBranchUpdateCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType
  >;
  updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry: (
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string
  ) => Promise<boolean>;

  buildDefaultBranchCheckSuiteReevaluationCallbacks: () => DefaultBranchCheckSuiteReevaluationCallbacks<
    ContextType,
    RepoInfoType,
    SequentialRegistryPrResultType
  >;
  buildDefaultBranchDirectPrReevaluationCallbacks: () => DefaultBranchDirectPrReevaluationCallbacks<
    ContextType,
    RepoInfoType,
    SequentialRegistryPrResultType
  >;
  reevaluateOpenDirectPullRequestsAfterDefaultBranchPush: (
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string,
    reason?: string
  ) => Promise<SequentialRegistryPrResultType>;
  maybeHandleDefaultBranchCheckSuiteSuccess: (
    context: ContextType,
    payload: unknown,
    checkSuite: CheckSuiteType | null,
    repoInfo: RepoInfoType
  ) => Promise<void>;

  buildCheckCompletedHandlerCallbacks: () => CheckCompletedHandlerCallbacks<
    ContextType,
    RepoInfoType,
    CheckRunType,
    CheckSuiteType,
    {
      byFile: Map<string, string[]>;
      machineReadableSources: RegistryValidationMachineReadableSource[];
    },
    RegistryValidationMachineReadableSource
  >;
};

export function createCheckWorkflowRuntime<
  ContextType extends ResourceBotContextBase,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  PullRequestFileType extends PullRequestFileLikeBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  SequentialRegistryPrResultType extends SequentialRegistryPrResultBase,
>(
  dependencies: CheckWorkflowRuntimeDependencies<
    ContextType,
    RepoInfoType,
    PullRequestType,
    PullRequestFileType,
    CheckRunType,
    CheckSuiteType,
    SequentialRegistryPrResultType
  >
): CheckWorkflowRuntime<
  ContextType,
  RepoInfoType,
  PullRequestType,
  PullRequestFileType,
  CheckRunType,
  CheckSuiteType,
  SequentialRegistryPrResultType
> {
  function buildWorkflowApprovalCallbacks(): WorkflowApprovalCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    PullRequestFileType
  > {
    return composeWorkflowApprovalCallbacks<ContextType, RepoInfoType, PullRequestType, PullRequestFileType>({
      isPullRequestOpen: dependencies.isPullRequestOpen,
      isSafeRegistryWorkflowApprovalFile: dependencies.isSafeRegistryWorkflowApprovalFile,
      listChangedFilesForPr: dependencies.listChangedFilesForPr,
      parseLinkedIssueNumberFromPr: dependencies.parseLinkedIssueNumberFromPr,
      isSnapshotManagedRequestPr: dependencies.isSnapshotManagedRequestPr,
      evaluateDirectPrOnApproval: dependencies.evaluateDirectPrOnApproval,
      hasAllowedStandaloneDirectPrApprovalForCurrentHead:
        dependencies.hasAllowedStandaloneDirectPrApprovalForCurrentHead,
      readFreshPullRequest: dependencies.readFreshPullRequest,
      isPlainObject: dependencies.isPlainObject,
      log: dependencies.log,
      getErrorMessage: dependencies.getErrorMessage,
      getHttpStatus: dependencies.getHttpStatus,
      toStringTrim: dependencies.toStringTrim,
    });
  }

  async function maybeApprovePendingWorkflowRunsForRegistryPrWithRetry(
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ): Promise<boolean> {
    return await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      context,
      repoInfo,
      pr,
      reason,
      buildWorkflowApprovalCallbacks()
    );
  }

  async function maybeApprovePendingWorkflowRunsForPrNumbers(
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumbers: number[],
    headSha: string,
    reason: string
  ): Promise<boolean> {
    return await maybeApprovePendingWorkflowRunsForPrNumbersApplication(
      context,
      repoInfo,
      prNumbers,
      headSha,
      reason,
      buildWorkflowApprovalCallbacks()
    );
  }

  function buildDefaultBranchApprovedPrBranchUpdateCallbacks(): DefaultBranchApprovedPrBranchUpdateCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType
  > {
    return composeDefaultBranchApprovedPrBranchUpdateCallbacks<ContextType, RepoInfoType, PullRequestType>({
      isSequentialRegistryPrActiveBlocking: dependencies.isSequentialRegistryPrActiveBlocking,
      listOpenPullRequests: dependencies.listOpenPullRequests,
      isSequentialRegistryPrHeadSkipped: dependencies.isSequentialRegistryPrHeadSkipped,
      listChangedYamlFilesForPrWithFallback: dependencies.listChangedYamlFilesForPrWithFallback,
      isSnapshotManagedRequestPr: dependencies.isSnapshotManagedRequestPr,
      isPullRequestApprovedForBranchMaintenance: dependencies.isPullRequestApprovedForBranchMaintenance,
      waitForPullRequestMergeability: dependencies.waitForPullRequestMergeability,
      isPullRequestOpen: dependencies.isPullRequestOpen,
      isPullRequestDirty: dependencies.isPullRequestDirty,
      readMergeableState: dependencies.readMergeableState,
      shouldUpdatePullRequestBranch: dependencies.shouldUpdatePullRequestBranch,
      requestPullRequestBranchUpdate: dependencies.requestPullRequestBranchUpdate,
      markSequentialRegistryPrHeadSkipped: dependencies.markSequentialRegistryPrHeadSkipped,
      getErrorMessage: dependencies.getErrorMessage,
      log: dependencies.log,
    });
  }

  async function updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry(
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string
  ): Promise<boolean> {
    return await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetryApplication(
      context,
      repoInfo,
      baseBranch,
      DEFAULT_BRANCH_UPDATE_RETRY_DELAY_MS,
      buildDefaultBranchApprovedPrBranchUpdateCallbacks()
    );
  }

  function buildDefaultBranchCheckSuiteReevaluationCallbacks(): DefaultBranchCheckSuiteReevaluationCallbacks<
    ContextType,
    RepoInfoType,
    SequentialRegistryPrResultType
  > {
    return composeDefaultBranchCheckSuiteReevaluationCallbacks<
      ContextType,
      RepoInfoType,
      SequentialRegistryPrResultType
    >({
      readDefaultBranchFromPayload: dependencies.readDefaultBranchFromPayload,
      getErrorMessage: dependencies.getErrorMessage,
      getHttpStatus: dependencies.getHttpStatus,
      getStaticConfig: async (context: ContextType, options: { forceReload: true }): Promise<unknown> =>
        await dependencies.getStaticConfig(context, options),
      reevaluateOpenDirectPullRequestsAfterDefaultBranchPush,
      updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry,
      log: dependencies.log,
    });
  }

  function buildDefaultBranchDirectPrReevaluationCallbacks(): DefaultBranchDirectPrReevaluationCallbacks<
    ContextType,
    RepoInfoType,
    SequentialRegistryPrResultType
  > {
    return composeDefaultBranchDirectPrReevaluationCallbacks<ContextType, RepoInfoType, SequentialRegistryPrResultType>(
      {
        runOneSequentialDirectRegistryPrMaintenance: dependencies.runOneSequentialDirectRegistryPrMaintenance,
        log: dependencies.log,
      }
    );
  }

  async function reevaluateOpenDirectPullRequestsAfterDefaultBranchPush(
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string,
    reason = 'default-branch-push:direct-pr-reevaluation'
  ): Promise<SequentialRegistryPrResultType> {
    return await reevaluateOpenDirectPullRequestsAfterDefaultBranchPushApplication(
      context,
      repoInfo,
      baseBranch,
      buildDefaultBranchDirectPrReevaluationCallbacks(),
      reason
    );
  }

  async function maybeHandleDefaultBranchCheckSuiteSuccess(
    context: ContextType,
    payload: unknown,
    checkSuite: CheckSuiteType | null,
    repoInfo: RepoInfoType
  ): Promise<void> {
    await maybeHandleDefaultBranchCheckSuiteSuccessApplication(
      context,
      payload,
      checkSuite,
      repoInfo,
      buildDefaultBranchCheckSuiteReevaluationCallbacks()
    );
  }

  function buildCheckCompletedHandlerCallbacks(): CheckCompletedHandlerCallbacks<
    ContextType,
    RepoInfoType,
    CheckRunType,
    CheckSuiteType,
    {
      byFile: Map<string, string[]>;
      machineReadableSources: RegistryValidationMachineReadableSource[];
    },
    RegistryValidationMachineReadableSource
  > {
    return composeCheckCompletedHandlerCallbacks<
      ContextType,
      RepoInfoType,
      CheckRunType,
      CheckSuiteType,
      {
        byFile: Map<string, string[]>;
        machineReadableSources: RegistryValidationMachineReadableSource[];
      },
      RegistryValidationMachineReadableSource
    >({
      readCheckRunFromPayload: dependencies.readCheckRunFromPayload,
      readCheckSuiteFromPayload: dependencies.readCheckSuiteFromPayload,
      readRepoInfoFromPayload: dependencies.readRepoInfoFromPayload,
      readCheckRunPrNumbers: dependencies.readCheckRunPrNumbers,
      resolveCheckSuitePrNumbers: dependencies.resolveCheckSuitePrNumbers,
      readCheckSuiteId: dependencies.readCheckSuiteId,
      listAllCheckRunsForSuite: dependencies.listAllCheckRunsForSuite,
      readCheckRunId: dependencies.readCheckRunId,
      readFirstRegistryValidationArtifactsForSuiteRuns: dependencies.readFirstRegistryValidationArtifactsForSuiteRuns,
      collapseBotCommentsByPrefix: dependencies.collapseBotCommentsByPrefix,
      postCheckSuiteRegistryValidationComments: dependencies.postCheckSuiteRegistryValidationComments,
      maybeHandleDefaultBranchCheckSuiteSuccess: maybeHandleDefaultBranchCheckSuiteSuccess as unknown as (
        context: ContextType,
        payload: unknown,
        checkSuite: CheckSuiteType | null,
        repoInfo: RepoInfoBase
      ) => Promise<void>,
      tryAutoMerge: dependencies.tryAutoMerge,
      maybeApprovePendingWorkflowRunsForPrNumbers: maybeApprovePendingWorkflowRunsForPrNumbers as unknown as (
        context: ContextType,
        repoInfo: RepoInfoBase,
        prNumbers: number[],
        headSha: string,
        reason: string
      ) => Promise<boolean>,
      handleBlockingRegistryHeadConclusion: dependencies.handleBlockingRegistryHeadConclusion,
      isBlockingCheckConclusion: dependencies.isBlockingCheckConclusion,
      readDefaultBranchFromPayload: dependencies.readDefaultBranchFromPayload,
      getStaticConfig: async (context: ContextType): Promise<unknown> => await dependencies.getStaticConfig(context),
      log: dependencies.log,
      isDebugEnabled: dependencies.isDebugEnabled,
      toStringTrim: dependencies.toStringTrim,
    });
  }

  return {
    buildWorkflowApprovalCallbacks,
    maybeApprovePendingWorkflowRunsForRegistryPrWithRetry,
    maybeApprovePendingWorkflowRunsForPrNumbers,
    buildDefaultBranchApprovedPrBranchUpdateCallbacks,
    updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry,
    buildDefaultBranchCheckSuiteReevaluationCallbacks,
    buildDefaultBranchDirectPrReevaluationCallbacks,
    reevaluateOpenDirectPullRequestsAfterDefaultBranchPush,
    maybeHandleDefaultBranchCheckSuiteSuccess,
    buildCheckCompletedHandlerCallbacks,
  };
}
