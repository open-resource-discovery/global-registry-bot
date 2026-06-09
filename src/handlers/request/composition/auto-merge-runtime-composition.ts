import {
  requestPullRequestBranchUpdateRespectingSequentialRegistryQueue as requestPullRequestBranchUpdateRespectingSequentialRegistryQueueApplication,
  type BranchUpdateSequentialHandoffCallbacks,
} from '../application/branch-update-sequential-handoff.js';
import {
  advanceSequentialRegistryPrQueueAfterTerminalState as advanceSequentialRegistryPrQueueAfterTerminalStateApplication,
  handleBlockingRegistryHeadConclusion as handleBlockingRegistryHeadConclusionApplication,
  releaseSequentialRegistryPrIfNotApprovedAfterGreen as releaseSequentialRegistryPrIfNotApprovedAfterGreenApplication,
  type SequentialRegistryPrTerminalCallbacks,
} from '../application/sequential-registry-pr-terminal.js';
import {
  runOneSequentialDirectRegistryPrMaintenance as runOneSequentialDirectRegistryPrMaintenanceApplication,
  type SequentialRegistryPrQueueCallbacks,
} from '../application/sequential-registry-pr-queue.js';
import {
  runAutoMergeEvaluation as runAutoMergeEvaluationApplication,
  tryAutoMerge as tryAutoMergeApplication,
  type AutoMergeTriggerCallbacks,
} from '../application/auto-merge-trigger.js';
import { tryMergeApprovedPrOrUpdateBranch as tryMergeApprovedPrOrUpdateBranchApplication } from '../application/merge-inflight.js';
import {
  runMergeApprovedPrOrUpdateBranch as runMergeApprovedPrOrUpdateBranchApplication,
  type MergeApprovedPrOrUpdateBranchCallbacks,
} from '../application/merge-approved-pr-or-update-branch.js';
import {
  processPullRequestForAutoMerge as processPullRequestForAutoMergeApplication,
  type PullRequestAutoMergeEntryCallbacks,
} from '../application/pull-request-auto-merge-entry.js';
import type { HeadGreenRunSummary } from '../domain/check-conclusions.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type IssueParamsBase = {
  owner: string;
  repo: string;
  issue_number: number;
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

type IssueLikeBase = {
  number: number;
  title?: string | null;
  body?: string | null;
  state?: string | null;
  user?: unknown | null;
  labels?: unknown;
};

type TemplateLikeBase = {
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type SequentialRegistryPrResultBase = {
  updated: boolean;
  processed: boolean;
  blockedByActive: boolean;
};

type SequentialRegistryPrActiveBase = {
  prNumber: number;
  startedHeadSha: string;
};

type HeadGreenEvaluationBase = {
  green: boolean;
  reason: string;
  latestRuns: HeadGreenRunSummary[];
  blockingRuns: HeadGreenRunSummary[];
};

type RuntimeLogLevel = 'debug' | 'info' | 'warn' | 'error';

export type AutoMergeRuntimeDependencies<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  PullRequestType extends PullRequestLikeBase,
  SequentialRegistryPrActiveType extends SequentialRegistryPrActiveBase | null,
  HeadGreenEvaluationType extends HeadGreenEvaluationBase,
> = {
  getStaticConfig: (context: ContextType, options?: { forceReload?: boolean }) => Promise<unknown>;
  evaluateHeadGreenForApprovalReevaluation: (
    context: ContextType,
    repoInfo: RepoInfoType,
    headSha: string
  ) => Promise<HeadGreenEvaluationType>;
  listOpenPullRequests: (context: ContextType, repoInfo: RepoInfoType) => Promise<PullRequestType[]>;
  readFreshPullRequest: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number
  ) => Promise<PullRequestType | null>;
  isSequentialDirectRegistryPr: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<boolean>;
  getSequentialRegistryPrActive: (repoInfo: RepoInfoType) => SequentialRegistryPrActiveType;
  clearSequentialRegistryPrActive: (repoInfo: RepoInfoType) => void;
  markSequentialRegistryPrHeadSkipped: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => void;
  markSequentialRegistryPrActive: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => void;
  requestPullRequestBranchUpdate: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<boolean>;
  isSequentialRegistryPrActiveBlocking: (context: ContextType, repoInfo: RepoInfoType) => Promise<boolean>;
  parseLinkedIssueNumberFromPr: (pr: PullRequestType, repoInfo?: RepoInfoType) => number | null;
  isSnapshotManagedRequestPr: (pr: PullRequestType) => boolean;
  pullRequestTargetsBranch: (pr: PullRequestType, branchName: string) => boolean;
  isSequentialRegistryPrHeadSkipped: (repoInfo: RepoInfoType, pr: PullRequestType) => boolean;
  listChangedYamlFilesForPrWithFallback: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<string[]>;
  shouldUpdatePullRequestBranch: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch: string
  ) => Promise<boolean>;
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
  hasAutoApprovedPrHead: (repoInfo: RepoInfoType, prNumber: number, headSha: string) => boolean;
  isCrossRepositoryPullRequest: (pr: PullRequestType, baseRepoInfo: RepoInfoType) => boolean;
  tryMergeIfGreen: (
    context: ContextType,
    args: {
      owner: string;
      repo: string;
      prNumber: number;
      mergeMethod: 'merge' | 'squash' | 'rebase';
      prData: PullRequestType;
    }
  ) => Promise<boolean | void>;
  maybeHandleStandaloneDirectPrApproval: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    options?: { baseBranch?: string }
  ) => Promise<'approved' | 'rejected' | 'continue'>;
  buildIssueParams: (repoInfo: RepoInfoType, issueNumber: number) => IssueParamsType;
  readLinkedIssue: (context: ContextType, params: IssueParamsType) => Promise<IssueType>;
  isPullRequestOpen: (pr: PullRequestType | null | undefined) => boolean;
  log: (context: ContextType, level: RuntimeLogLevel, metadata: unknown, message: string) => void;
  getErrorMessage: (error: unknown) => string;
  getHttpStatus: (error: unknown) => number | undefined;
  hasIssueFormInputs: (issue: IssueType | null | undefined) => boolean;
  loadTemplateWithLabelRefresh: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType
  ) => Promise<TemplateType>;
  parseForm: (body: string, template: TemplateType) => FormDataType;
  readIssueBodyForProcessing: (issueBody: unknown) => string;
  isRequestIssue: (context: ContextType, template: TemplateType, parsedFormData: FormDataType) => boolean;
  buildCompatibleRequestSnapshotHashes: (
    issueBody: unknown,
    parsedFormData: FormDataType,
    template: TemplateType
  ) => string[];
  calcSnapshotHash: (formData: FormDataType, template: TemplateType, rawBody: string) => string;
  extractHashFromPrBody: (body: string) => string;
  closeOutdatedRequestPrs: (
    context: ContextType,
    params: IssueParamsType,
    template: TemplateType,
    options?: { parsedFormData?: FormDataType; currentHash?: string; acceptedHashes?: string[] }
  ) => Promise<void>;
  maybeHandleDirectPrApprovalForMerge: (
    context: ContextType,
    repoInfo: RepoInfoType,
    issueParams: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    pr: PullRequestType
  ) => Promise<'approved' | 'rejected' | 'continue'>;
};

export type AutoMergeRuntime<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  SequentialRegistryPrResultType extends SequentialRegistryPrResultBase,
> = {
  requestPullRequestBranchUpdateRespectingSequentialRegistryQueue: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch: string,
    reason: string
  ) => Promise<boolean>;
  advanceSequentialRegistryPrQueueAfterTerminalState: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<void>;
  runOneSequentialDirectRegistryPrMaintenance: (
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string,
    reason: string
  ) => Promise<SequentialRegistryPrResultType>;
  runAutoMergeEvaluation: (context: ContextType, repoInfo: RepoInfoType, normalizedHeadSha: string) => Promise<boolean>;
  tryAutoMerge: (context: ContextType, repoInfo: RepoInfoType, headSha: string) => Promise<void>;
  tryMergeApprovedPrOrUpdateBranch: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<void>;
  processPullRequestForAutoMerge: (context: ContextType, repoInfo: RepoInfoType, pr: PullRequestType) => Promise<void>;
  handleBlockingRegistryHeadConclusion: (
    context: ContextType,
    repoInfo: RepoInfoType,
    headSha: string,
    baseBranch: string,
    reason: string
  ) => Promise<boolean>;
};

export function createAutoMergeRuntime<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  PullRequestType extends PullRequestLikeBase,
  SequentialRegistryPrResultType extends SequentialRegistryPrResultBase,
  SequentialRegistryPrActiveType extends SequentialRegistryPrActiveBase | null,
  HeadGreenEvaluationType extends HeadGreenEvaluationBase,
>(
  dependencies: AutoMergeRuntimeDependencies<
    ContextType,
    RepoInfoType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    PullRequestType,
    SequentialRegistryPrActiveType,
    HeadGreenEvaluationType
  >
): AutoMergeRuntime<ContextType, RepoInfoType, PullRequestType, SequentialRegistryPrResultType> {
  const logInfoOrWarn = (context: ContextType, level: 'info' | 'warn', metadata: unknown, message: string): void => {
    dependencies.log(context, level, metadata, message);
  };

  const logAnyLevel = (context: ContextType, level: RuntimeLogLevel, metadata: unknown, message: string): void => {
    dependencies.log(context, level, metadata, message);
  };

  async function shouldDeferSequentialDirectRegistryPrProcessing(
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType
  ): Promise<boolean> {
    const active = dependencies.getSequentialRegistryPrActive(repoInfo);
    if (!active || active.prNumber === pr.number) return false;

    if (!(await dependencies.isSequentialRegistryPrActiveBlocking(context, repoInfo))) {
      return false;
    }

    const currentActive = dependencies.getSequentialRegistryPrActive(repoInfo);
    if (!currentActive || currentActive.prNumber === pr.number) {
      return false;
    }

    dependencies.log(
      context,
      'info',
      {
        prNumber: pr.number,
        activePrNumber: currentActive.prNumber,
        activeHeadSha: currentActive.startedHeadSha,
      },
      'sequential-registry-pr:auto-merge-deferred'
    );

    return true;
  }

  function buildBranchUpdateSequentialHandoffCallbacks(): BranchUpdateSequentialHandoffCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    SequentialRegistryPrActiveType
  > {
    return {
      isSequentialDirectRegistryPr: dependencies.isSequentialDirectRegistryPr,
      requestPullRequestBranchUpdate: dependencies.requestPullRequestBranchUpdate,
      getSequentialRegistryPrActive: dependencies.getSequentialRegistryPrActive,
      markSequentialRegistryPrActive: dependencies.markSequentialRegistryPrActive,
      runOneSequentialDirectRegistryPrMaintenance,
    };
  }

  async function requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch: string,
    reason: string
  ): Promise<boolean> {
    return await requestPullRequestBranchUpdateRespectingSequentialRegistryQueueApplication(
      context,
      repoInfo,
      pr,
      baseBranch,
      reason,
      buildBranchUpdateSequentialHandoffCallbacks()
    );
  }

  function buildSequentialRegistryPrTerminalCallbacks(): SequentialRegistryPrTerminalCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    SequentialRegistryPrActiveType,
    HeadGreenEvaluationType
  > {
    return {
      readFreshPullRequest: dependencies.readFreshPullRequest,
      isPullRequestOpen: dependencies.isPullRequestOpen,
      getSequentialRegistryPrActive: dependencies.getSequentialRegistryPrActive,
      clearSequentialRegistryPrActive: dependencies.clearSequentialRegistryPrActive,
      markSequentialRegistryPrHeadSkipped: dependencies.markSequentialRegistryPrHeadSkipped,
      listOpenPullRequests: dependencies.listOpenPullRequests,
      pullRequestTargetsBranch: dependencies.pullRequestTargetsBranch,
      listChangedYamlFilesForPrWithFallback: dependencies.listChangedYamlFilesForPrWithFallback,
      runOneSequentialDirectRegistryPrMaintenance,
      evaluateHeadGreenForApprovalReevaluation: dependencies.evaluateHeadGreenForApprovalReevaluation,
      isPullRequestApprovedForBranchMaintenance: dependencies.isPullRequestApprovedForBranchMaintenance,
      log: logInfoOrWarn,
    };
  }

  async function advanceSequentialRegistryPrQueueAfterTerminalState(
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ): Promise<void> {
    await advanceSequentialRegistryPrQueueAfterTerminalStateApplication(
      context,
      repoInfo,
      pr,
      reason,
      buildSequentialRegistryPrTerminalCallbacks()
    );
  }

  function buildSequentialRegistryPrQueueCallbacks(): SequentialRegistryPrQueueCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    HeadGreenEvaluationType
  > {
    return {
      isSequentialRegistryPrActiveBlocking: dependencies.isSequentialRegistryPrActiveBlocking,
      listOpenPullRequests: dependencies.listOpenPullRequests,
      parseLinkedIssueNumberFromPr: dependencies.parseLinkedIssueNumberFromPr,
      isSnapshotManagedRequestPr: dependencies.isSnapshotManagedRequestPr,
      pullRequestTargetsBranch: dependencies.pullRequestTargetsBranch,
      isSequentialRegistryPrHeadSkipped: dependencies.isSequentialRegistryPrHeadSkipped,
      listChangedYamlFilesForPrWithFallback: dependencies.listChangedYamlFilesForPrWithFallback,
      readFreshPullRequest: dependencies.readFreshPullRequest,
      shouldUpdatePullRequestBranch: dependencies.shouldUpdatePullRequestBranch,
      isPullRequestApprovedForBranchMaintenance: dependencies.isPullRequestApprovedForBranchMaintenance,
      requestPullRequestBranchUpdate: dependencies.requestPullRequestBranchUpdate,
      markSequentialRegistryPrActive: dependencies.markSequentialRegistryPrActive,
      markSequentialRegistryPrHeadSkipped: dependencies.markSequentialRegistryPrHeadSkipped,
      evaluateHeadGreenForApprovalReevaluation: dependencies.evaluateHeadGreenForApprovalReevaluation,
      processPullRequestForAutoMerge,
      log: dependencies.log,
    };
  }

  async function runOneSequentialDirectRegistryPrMaintenance(
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string,
    reason: string
  ): Promise<SequentialRegistryPrResultType> {
    return (await runOneSequentialDirectRegistryPrMaintenanceApplication(
      context,
      repoInfo,
      baseBranch,
      reason,
      buildSequentialRegistryPrQueueCallbacks()
    )) as SequentialRegistryPrResultType;
  }

  async function handleBlockingRegistryHeadConclusion(
    context: ContextType,
    repoInfo: RepoInfoType,
    headSha: string,
    baseBranch: string,
    reason: string
  ): Promise<boolean> {
    return await handleBlockingRegistryHeadConclusionApplication(
      context,
      repoInfo,
      headSha,
      baseBranch,
      reason,
      buildSequentialRegistryPrTerminalCallbacks()
    );
  }

  async function releaseSequentialRegistryPrIfNotApprovedAfterGreen(
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType
  ): Promise<void> {
    await releaseSequentialRegistryPrIfNotApprovedAfterGreenApplication(
      context,
      repoInfo,
      pr,
      buildSequentialRegistryPrTerminalCallbacks()
    );
  }

  function buildMergeApprovedPrOrUpdateBranchCallbacks(): MergeApprovedPrOrUpdateBranchCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType
  > {
    return {
      waitForPullRequestMergeability: dependencies.waitForPullRequestMergeability,
      shouldUpdatePullRequestBranch: dependencies.shouldUpdatePullRequestBranch,
      requestPullRequestBranchUpdateRespectingSequentialRegistryQueue,
      hasAutoApprovedPrHead: dependencies.hasAutoApprovedPrHead,
      isPullRequestApprovedForBranchMaintenance: dependencies.isPullRequestApprovedForBranchMaintenance,
      isCrossRepositoryPullRequest: dependencies.isCrossRepositoryPullRequest,
      evaluateHeadGreenForApprovalReevaluation: dependencies.evaluateHeadGreenForApprovalReevaluation,
      tryMergeIfGreen: dependencies.tryMergeIfGreen,
      readFreshPullRequest: dependencies.readFreshPullRequest,
      log: logInfoOrWarn,
      getErrorMessage: dependencies.getErrorMessage,
      getHttpStatus: dependencies.getHttpStatus,
    };
  }

  async function runMergeApprovedPrOrUpdateBranch(
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ): Promise<void> {
    await runMergeApprovedPrOrUpdateBranchApplication(
      context,
      repoInfo,
      pr,
      reason,
      buildMergeApprovedPrOrUpdateBranchCallbacks()
    );
  }

  async function tryMergeApprovedPrOrUpdateBranch(
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ): Promise<void> {
    await tryMergeApprovedPrOrUpdateBranchApplication(context, repoInfo, pr, reason, runMergeApprovedPrOrUpdateBranch);
  }

  function buildPullRequestAutoMergeEntryCallbacks(): PullRequestAutoMergeEntryCallbacks<
    ContextType,
    RepoInfoType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    PullRequestType
  > {
    return {
      isSequentialDirectRegistryPr: dependencies.isSequentialDirectRegistryPr,
      shouldDeferSequentialDirectRegistryPrProcessing,
      parseLinkedIssueNumberFromPr: dependencies.parseLinkedIssueNumberFromPr,
      readFreshPullRequest: dependencies.readFreshPullRequest,
      maybeHandleStandaloneDirectPrApproval: dependencies.maybeHandleStandaloneDirectPrApproval,
      tryMergeApprovedPrOrUpdateBranch,
      buildIssueParams: dependencies.buildIssueParams,
      readLinkedIssue: dependencies.readLinkedIssue,
      log: logAnyLevel,
      getErrorMessage: dependencies.getErrorMessage,
      getHttpStatus: dependencies.getHttpStatus,
      isCrossRepositoryPullRequest: dependencies.isCrossRepositoryPullRequest,
      hasIssueFormInputs: dependencies.hasIssueFormInputs,
      loadTemplateWithLabelRefresh: dependencies.loadTemplateWithLabelRefresh,
      parseForm: dependencies.parseForm,
      readIssueBodyForProcessing: dependencies.readIssueBodyForProcessing,
      isRequestIssue: dependencies.isRequestIssue,
      buildCompatibleRequestSnapshotHashes: dependencies.buildCompatibleRequestSnapshotHashes,
      calcSnapshotHash: dependencies.calcSnapshotHash,
      extractHashFromPrBody: dependencies.extractHashFromPrBody,
      closeOutdatedRequestPrs: dependencies.closeOutdatedRequestPrs,
      maybeHandleDirectPrApprovalForMerge: dependencies.maybeHandleDirectPrApprovalForMerge,
    };
  }

  async function processPullRequestForAutoMerge(
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType
  ): Promise<void> {
    await processPullRequestForAutoMergeApplication(context, repoInfo, pr, buildPullRequestAutoMergeEntryCallbacks());
  }

  function buildAutoMergeTriggerCallbacks(): AutoMergeTriggerCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    SequentialRegistryPrActiveType,
    HeadGreenEvaluationType
  > {
    return {
      getStaticConfig: dependencies.getStaticConfig,
      evaluateHeadGreenForApprovalReevaluation: dependencies.evaluateHeadGreenForApprovalReevaluation,
      listOpenPullRequests: dependencies.listOpenPullRequests,
      processPullRequestForAutoMerge,
      releaseSequentialRegistryPrIfNotApprovedAfterGreen,
      advanceSequentialRegistryPrQueueAfterTerminalState,
      readFreshPullRequest: dependencies.readFreshPullRequest,
      isSequentialDirectRegistryPr: dependencies.isSequentialDirectRegistryPr,
      getSequentialRegistryPrActive: dependencies.getSequentialRegistryPrActive,
      clearSequentialRegistryPrActive: dependencies.clearSequentialRegistryPrActive,
      markSequentialRegistryPrHeadSkipped: dependencies.markSequentialRegistryPrHeadSkipped,
      runOneSequentialDirectRegistryPrMaintenance,
      log: logAnyLevel,
    };
  }

  async function runAutoMergeEvaluation(
    context: ContextType,
    repoInfo: RepoInfoType,
    normalizedHeadSha: string
  ): Promise<boolean> {
    return await runAutoMergeEvaluationApplication(
      context,
      repoInfo,
      normalizedHeadSha,
      buildAutoMergeTriggerCallbacks()
    );
  }

  async function tryAutoMerge(context: ContextType, repoInfo: RepoInfoType, headSha: string): Promise<void> {
    await tryAutoMergeApplication(context, repoInfo, headSha, runAutoMergeEvaluation, buildAutoMergeTriggerCallbacks());
  }

  return {
    requestPullRequestBranchUpdateRespectingSequentialRegistryQueue,
    advanceSequentialRegistryPrQueueAfterTerminalState,
    runOneSequentialDirectRegistryPrMaintenance,
    runAutoMergeEvaluation,
    tryAutoMerge,
    tryMergeApprovedPrOrUpdateBranch,
    processPullRequestForAutoMerge,
    handleBlockingRegistryHeadConclusion,
  };
}
