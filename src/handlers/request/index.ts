import { setStateLabel as setStateLabelRaw } from './state.js';
import { postOnce as postOnceRaw, collapseBotCommentsByPrefix as collapseBotCommentsByPrefixRaw } from './comments.js';
import { loadTemplate as loadTemplateRaw, parseForm as parseFormRaw } from './template.js';
import {
  validateRequestIssue as validateRequestIssueRaw,
  runApprovalHook as runApprovalHookRaw,
} from './validation/run.js';
import {
  calcSnapshotHash as calcSnapshotHashRaw,
  extractHashFromPrBody as extractHashFromPRBodyRaw,
  findOpenIssuePrs as findOpenIssuePRsRaw,
} from './pr/snapshot.js';
import { createRequestPr as createRequestPRRaw } from './pr/create.js';
import { buildReviewHandoverBody as buildReviewHandoverBodyPure } from './domain/review-handover-rendering.js';
import {
  ensureAutomatedApprovalReviewForCurrentHead as ensureAutomatedApprovalReviewForCurrentHeadApplication,
  type AutomatedApprovalReviewCallbacks,
  type AutomatedApprovalReviewOptions,
} from './application/automated-approval-review.js';
import {
  autoApprovedPrHeadKey as autoApprovedPrHeadKeyApplication,
  hasAutoApprovedPrHead as hasAutoApprovedPrHeadApplication,
  markAutoApprovedPrHead as markAutoApprovedPrHeadApplication,
} from './application/auto-approved-head-tracking.js';
import {
  hasAutoApprovalReviewForHead as hasAutoApprovalReviewForHeadApplication,
  type AutoApprovalReviewDetectionCallbacks,
} from './application/auto-approval-review-detection.js';
import {
  hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr as hasAllowedCurrentHeadManualApprovalForStandaloneDirectPrApplication,
  hasAllowedStandaloneDirectPrApprovalForCurrentHead as hasAllowedStandaloneDirectPrApprovalForCurrentHeadApplication,
  type DirectPrReviewApprovalCallbacks,
} from './application/direct-pr-review-approval.js';
import { type DirectPrChangedResourceApprovalCallbacks } from './application/direct-pr-changed-resource-approval.js';
import {
  evaluateDirectPrOnApproval as evaluateDirectPrOnApprovalApplication,
  type DirectPrApprovalEvaluationCallbacks,
} from './application/direct-pr-approval-evaluation.js';
import {
  evaluateHeadGreenForApprovalReevaluation as evaluateHeadGreenForApprovalReevaluationApplication,
  type HeadGreenEvaluationCallbacks,
} from './application/head-green-evaluation.js';
import {
  isPullRequestApprovedForBranchMaintenance as isPullRequestApprovedForBranchMaintenanceApplication,
  type BranchMaintenanceApprovalCallbacks,
} from './application/branch-maintenance-approval.js';
import {
  runBranchUpdateBenignFailureRetry as runBranchUpdateBenignFailureRetryApplication,
  type BranchUpdateBenignRetryCallbacks,
  type BranchUpdateBenignRetryOutcome,
} from './application/branch-update-benign-retry.js';
import {
  requestPullRequestBranchUpdate as requestPullRequestBranchUpdateApplication,
  type BranchUpdateOrchestrationCallbacks,
} from './application/branch-update-orchestration.js';
import {
  readCheckRunFromPayload as readCheckRunFromPayloadApplication,
  readCheckRunPrNumbers as readCheckRunPrNumbersApplication,
  readCheckSuiteFromPayload as readCheckSuiteFromPayloadApplication,
  readCheckSuiteId as readCheckSuiteIdApplication,
  resolveCheckSuitePrNumbers as resolveCheckSuitePrNumbersApplication,
  type CheckPrResolutionCallbacks,
} from './application/check-pr-resolution.js';
import {
  listAllCheckRunsForSuite as listAllCheckRunsForSuiteApplication,
  readFirstRegistryValidationArtifactsForSuiteRuns as readFirstRegistryValidationArtifactsForSuiteRunsApplication,
  type CheckSuiteAnnotationsCallbacks,
} from './application/check-suite-annotations.js';
import {
  postCheckSuiteRegistryValidationComments as postCheckSuiteRegistryValidationCommentsApplication,
  type CheckSuiteCiCommentingCallbacks,
} from './application/check-suite-ci-commenting.js';
import {
  handleCheckCompletedEvent as handleCheckCompletedEventApplication,
  type CheckCompletedHandlerCallbacks,
} from './application/check-completed-handler.js';
import {
  maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication,
  maybeApprovePendingWorkflowRunsForPrNumbersApplication,
  type WorkflowApprovalCallbacks,
} from './application/workflow-approval.js';
import { extractParentContactCandidates, lookupGithubLoginsByEmail } from './application/parent-contact-resolution.js';
import { checkParentChainExistsInFlatStructureApplication } from './application/parent-chain-validation.js';
import {
  maybeHandleDefaultBranchCheckSuiteSuccess as maybeHandleDefaultBranchCheckSuiteSuccessApplication,
  type DefaultBranchCheckSuiteReevaluationCallbacks,
} from './application/default-branch-check-suite-reevaluation.js';
import {
  reevaluateOpenDirectPullRequestsAfterDefaultBranchPush as reevaluateOpenDirectPullRequestsAfterDefaultBranchPushApplication,
  type DefaultBranchDirectPrReevaluationCallbacks,
} from './application/default-branch-direct-pr-reevaluation.js';
import {
  updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry as updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetryApplication,
  type DefaultBranchApprovedPrBranchUpdateCallbacks,
} from './application/default-branch-approved-pr-branch-update.js';
import {
  processAuthorUpdateComment as processAuthorUpdateCommentApplication,
  processRequestIssueLifecycle as processRequestIssueLifecycleApplication,
  type RequestIssueAuthorUpdateCallbacks,
  type RequestIssueLifecycleCallbacks,
} from './application/request-issue-lifecycle.js';
import {
  closeOutdatedRequestPrs as closeOutdatedRequestPrsApplication,
  type OutdatedRequestPrCleanupCallbacks,
} from './application/outdated-request-pr-cleanup.js';
import {
  createRequestPrWithRecovery as createRequestPrWithRecoveryApplication,
  type RequestPrCreationRecoveryCallbacks,
} from './application/request-pr-creation-recovery.js';
import {
  addApprovedLabelToPr as addApprovedLabelToPrApplication,
  applyApprovedRequestState as applyApprovedRequestStateApplication,
  ensureAssigneesPresent as ensureAssigneesPresentApplication,
  ensureLabelsPresentOnce as ensureLabelsPresentOnceApplication,
  ensureReviewLabelsPresentOnIssue as ensureReviewLabelsPresentOnIssueApplication,
  fetchIssueLabels as fetchIssueLabelsApplication,
  removeExactLabelsFromIssue as removeExactLabelsFromIssueApplication,
  removeProgressStatusLabels as removeProgressStatusLabelsApplication,
  removeRejectedStatusLabel as removeRejectedStatusLabelApplication,
  type IssueStateReviewerOperationsCallbacks,
} from './application/issue-state-reviewer-operations.js';
import {
  buildRegistryValidationAggregatePrCommentBody as buildRegistryValidationAggregatePrCommentBodyApplication,
  type RequestValidationPostingCallbacks,
} from './application/request-validation-posting.js';
import {
  buildPullRequestHeadReadCandidates as buildPullRequestHeadReadCandidatesApplication,
  isChangedYamlCandidate as isChangedYamlCandidateApplication,
  listChangedYamlFilesForPr as listChangedYamlFilesForPrApplication,
  listChangedYamlFilesForPrAgainstCurrentBase as listChangedYamlFilesForPrAgainstCurrentBaseApplication,
  listChangedYamlFilesForPrWithFallback as listChangedYamlFilesForPrWithFallbackApplication,
  listChangedYamlFilesPage as listChangedYamlFilesPageApplication,
  type PullRequestHeadReadCandidate,
  readPullRequestHeadFileText as readPullRequestHeadFileTextApplication,
  readPullRequestHeadTreeEntries as readPullRequestHeadTreeEntriesApplication,
  readRecursiveGitTreeEntries as readRecursiveGitTreeEntriesApplication,
  readRepoFileTextAtRef as readRepoFileTextAtRefApplication,
  registryYamlTreeEntryPath as registryYamlTreeEntryPathApplication,
} from './application/pr-head-changed-file-discovery.js';
import { readRegistryDocForApproval as readRegistryDocForApprovalApplication } from './application/registry-doc-for-approval.js';
import { isSequentialDirectRegistryPr as isSequentialDirectRegistryPrApplication } from './application/sequential-direct-registry-pr-detection.js';
import {
  clearSequentialRegistryPrActive,
  getSequentialRegistryPrActive,
  isSequentialRegistryPrHeadSkipped,
  markSequentialRegistryPrActive as markSequentialRegistryPrActiveState,
  markSequentialRegistryPrHeadSkipped as markSequentialRegistryPrHeadSkippedState,
} from './application/sequential-registry-pr-state.js';
import { callPullRequestBranchUpdate as callPullRequestBranchUpdateApplication } from './application/pull-request-branch-update-call.js';
import {
  waitForPullRequestMergeability as waitForPullRequestMergeabilityApplication,
  type PullRequestMergeabilityCallbacks,
} from './application/pull-request-mergeability.js';
import {
  resolvePullRequestRequestAuthorId as resolvePullRequestRequestAuthorIdApplication,
  type PullRequestAuthorResolutionCallbacks,
} from './application/pull-request-author-resolution.js';
import {
  resolveAllowedApproversForRequestTypes as resolveAllowedApproversForRequestTypesApplication,
  type DirectPrApproverResolutionCallbacks,
} from './application/direct-pr-approver-resolution.js';
import {
  resolveDirectPrRequestTypes as resolveDirectPrRequestTypesApplication,
  type DirectPrRequestTypeResolutionCallbacks,
} from './application/direct-pr-request-type-resolution.js';
import { listPullRequestReviews as listPullRequestReviewsApplication } from './application/pull-request-review-reading.js';
import { handoverStandaloneDirectPrToReview } from './application/pr-review-handover.js';
import { handoverToCpa } from './application/review-handover.js';
import { rejectRequestFromApprovalHook } from './application/approval-rejection.js';
import { postApprovalRejectedOnce, postApprovalUnknownOnce } from './application/approval-outcome-posting.js';
import { type ApprovalCommentHandlingCallbacks } from './application/approval-comment-handling.js';
import { type OwnerApprovalCommentHandlingCallbacks } from './application/owner-approval-comment-handling.js';
import {
  maybeHandleStandaloneDirectPrApproval as maybeHandleStandaloneDirectPrApprovalApplication,
  type StandaloneDirectPrApprovalCallbacks,
} from './application/direct-pr-standalone-approval.js';
import {
  maybeHandleDirectPrApprovalForMerge as maybeHandleDirectPrApprovalForMergeApplication,
  type DirectPrLinkedIssueApprovalCallbacks,
} from './application/direct-pr-linked-issue-approval.js';
import {
  handleDirectPrApprovalComment as handleDirectPrApprovalCommentApplication,
  type DirectPrApprovalCommentHandlingCallbacks,
} from './application/direct-pr-approval-comment-handling.js';
import {
  detectSingleRoutingLabel as detectSingleRoutingLabelApplication,
  enforceRoutingLabelLock as enforceRoutingLabelLockApplication,
  ensureRoutingLockMarker as ensureRoutingLockMarkerApplication,
  handleClosedIssueWorkflowGuard as handleClosedIssueWorkflowGuardApplication,
  handleIssueLabelChangeWorkflowGuard as handleIssueLabelChangeWorkflowGuardApplication,
  type IssueWorkflowGuardCallbacks,
} from './application/issue-workflow-guard.js';
import {
  assignParentOwnersForApproval as assignParentOwnersForApprovalApplication,
  clearParentOwnerActionState as clearParentOwnerActionStateApplication,
  ensureContactApprovalMarker as ensureContactApprovalMarkerApplication,
  ensureParentApprovalMarker as ensureParentApprovalMarkerApplication,
  setParentOwnerActionState as setParentOwnerActionStateApplication,
  type OwnerApprovalRequirementsCallbacks,
} from './application/owner-approval-requirements.js';
import { type ApprovedRequestFinalizationCallbacks } from './application/approved-request-finalization.js';
import { isBlockingCheckConclusion, type HeadGreenRunSummary } from './domain/check-conclusions.js';
import {
  isPullRequestBehindBase as isPullRequestBehindBasePure,
  isPullRequestDirty as isPullRequestDirtyPure,
  isPullRequestOpen as isPullRequestOpenPure,
  readMergeableState as readMergeableStatePure,
} from './domain/pull-request-merge-state.js';
import {
  isCrossRepositoryPullRequest as isCrossRepositoryPullRequestPure,
  resolvePullRequestHeadRepoInfo as resolvePullRequestHeadRepoInfoPure,
  sameRepoInfo as sameRepoInfoPure,
} from './domain/pull-request-repo-info.js';
import {
  isPullRequestBehindCurrentBase as isPullRequestBehindCurrentBaseApplication,
  readBranchHeadSha as readBranchHeadShaApplication,
  type BranchUpdateDecisionCallbacks,
} from './application/branch-update-decision.js';
import {
  matchRequestTypesForFile as matchRequestTypesForFilePure,
  pickRequestTypeForChangedResource as pickRequestTypeForChangedResourcePure,
} from './domain/direct-pr-resource-mapping.js';
import { type ApprovalDecision } from './domain/approval-decision.js';
import { isAuthorizedApprover as isAuthorizedApproverPure } from './domain/approval-authorization.js';
import {
  normalizeLogin as normalizeLoginPure,
  toStringTrim as toStringTrimPure,
  uniqLogins as uniqLoginsPure,
} from './domain/login-utils.js';
import { buildAutoApprovalReviewMarker as buildAutoApprovalReviewMarkerPure } from './domain/auto-approval-review-marker.js';
import { getUnknownManualApprovers, getVisibleApprovalText } from './domain/approval-policy.js';
import {
  readContactApprovalMeta,
  readParentApprovalMeta,
  type ContactApprovalMeta,
  type ParentApprovalMeta,
} from './domain/approval-markers.js';
import { readIssueBodyForProcessing } from './domain/issue-body-processing.js';
import { type RegistryValidationMachineReadableSource } from './domain/registry-validation-annotations.js';
import { isApprovalComment, isAuthorUpdateComment, stripQuoteAndCode } from './domain/comment-commands.js';
import { tryMergeIfGreen as tryMergeIfGreenRaw } from '../../lib/auto-merge.js';
import { DEFAULT_CONFIG, type NormalizedStaticConfig, type RegistryBotHooks } from '../../config.js';
import { getDocLinksFromConfig } from './constants.js';
import { getErrorMessage, getHttpStatus, isPlainObject } from './infrastructure/errors.js';
import { log as infraLog } from './infrastructure/logger.js';
import {
  createGitHubGateway,
  createGitHubIssueUpdateGateway,
  type GitHubGateway,
} from './infrastructure/github-gateway.js';
import {
  readDefaultBranchFromPayload,
  readDefaultBranchFromPush,
  readPayloadLabelName,
  readPushChangedFiles,
  readRepoInfoFromPayload,
} from './infrastructure/request-context.js';
import { isRepoContentFile, readRepoFileText, readYamlFromRepo } from './infrastructure/repo-files.js';
import { createStaticConfigContextLoader } from './infrastructure/static-config-context.js';
import { registerRequestEvents } from './events/index.js';
import { createIssueCommentEventHandler } from './events/issue-comments.js';
import {
  createIssueClosedEventHandler,
  createIssueLabelChangeEventHandler,
  createIssueLifecycleEventHandler,
} from './events/issues.js';
import { createPullRequestEventHandler } from './events/pull-requests.js';
import { createCheckEventHandler } from './events/checks.js';
import { createStatusEventHandler } from './events/status.js';
import { createPushEventHandler } from './events/push.js';
import {
  composeApprovalCommentHandlingCallbacks,
  composeApprovedRequestFinalizationCallbacks,
  composeCheckCompletedHandlerCallbacks,
  createApprovalRuntime,
  createAutoMergeRuntime,
  composeDefaultBranchApprovedPrBranchUpdateCallbacks,
  composeDefaultBranchCheckSuiteReevaluationCallbacks,
  composeDefaultBranchDirectPrReevaluationCallbacks,
  composeDirectPrApprovalCommentHandlingCallbacks,
  composeIssueStateReviewerOperationsCallbacks,
  composeIssueWorkflowGuardCallbacks,
  composeOwnerApprovalCommentHandlingCallbacks,
  composeRequestIssueAuthorUpdateCallbacks,
  composeRequestIssueLifecycleCallbacks,
  composeRequestPrCreationRecoveryCallbacks,
  composeStandaloneDirectPrApprovalCallbacks,
  composeWorkflowApprovalCallbacks,
} from './composition/index.js';
import type { Context, Probot } from 'probot';
import { createHash } from 'node:crypto';

const DBG = process.env.DEBUG_NS === '1';

type RequestEvents =
  | 'issues.opened'
  | 'issues.edited'
  | 'issues.closed'
  | 'issues.reopened'
  | 'issues.labeled'
  | 'issues.unlabeled'
  | 'issue_comment.created'
  | 'issue_comment.edited'
  | 'pull_request.opened'
  | 'pull_request.synchronize'
  | 'pull_request.reopened'
  | 'pull_request.ready_for_review'
  | 'check_suite.completed'
  | 'check_run.completed'
  | 'status'
  | 'push';

type ResourceBotContextExt = {
  resourceBotConfig?: NormalizedStaticConfig;
  resourceBotHooks?: RegistryBotHooks | null;
  resourceBotHooksSource?: string | null;
};

type BotContext<E extends RequestEvents> = Context<E> & ResourceBotContextExt;

type RepoInfo = { owner: string; repo: string };
type IssueParams = { owner: string; repo: string; issue_number: number };

type LabelLike = string | { name?: string | null };
type UserLike = { login?: string | null };
type SenderLike = { type?: string | null; login?: string | null };

type IssueLike = {
  number: number;
  id?: number;
  title?: string | null;
  state?: string | null;
  body?: string | null;
  labels?: LabelLike[];
  user?: UserLike | null;
  created_at?: string | null;
  updated_at?: string | null;
};

type CommentLike = {
  body?: string | null;
  user: { login: string };
};

type TemplateMeta = {
  requestType?: string;
  root?: string;
  schema?: string;
  path?: string;
};

type TemplateLike = {
  _meta?: TemplateMeta;
  title?: string | null;
  name?: string | null;
  body?: unknown[];
  labels?: unknown[];
  [k: string]: unknown;
};

type FormData = Record<string, string>;

type PostOnceOptions = { minimizeTag?: string };

type CollapseBotCommentsByPrefixOptions = {
  perPage?: number;
  tagPrefix: string;
  keepTags?: string[];
  collapseBody?: string;
  classifier?: 'OUTDATED' | 'RESOLVED' | 'DUPLICATE' | 'OFF_TOPIC' | 'SPAM' | 'ABUSE';
};

type PullRequestRepoLike = {
  name?: string | null;
  full_name?: string | null;
  owner?: UserLike | null;
};

type PullRequestBranchLike = {
  ref: string;
  sha: string;
  repo?: PullRequestRepoLike | null;
};

type PullRequestLike = {
  number: number;
  title?: string | null;
  body?: string | null;
  state?: string | null;
  user?: UserLike | null;
  head: PullRequestBranchLike;
  base?: PullRequestBranchLike;

  mergeable?: boolean | null;
  mergeable_state?: string | null;
  draft?: boolean | null;
};

type PullRequestFileLike = {
  filename?: string | null;
  status?: string | null;
};

type PullRequestReviewLike = {
  id?: number | null;
  state?: string | null;
  body?: string | null;
  submitted_at?: string | null;
  user?: UserLike | null;
  commit_id?: string | null;
};

type CheckRunPullRequestRef = { number?: number | null };

type CheckRunLike = {
  id?: number | null;
  name?: string | null;
  status?: string | null;
  conclusion?: string | null;
  head_sha?: string | null;
  html_url?: string | null;
  pull_requests?: CheckRunPullRequestRef[] | null;
};

type GitTreeEntryLike = {
  path?: string | null;
  type?: string | null;
  sha?: string | null;
};

type DirectPrApprovalOptions = {
  baseBranch?: string;
};

type HeadGreenEvaluation = {
  green: boolean;
  reason: string;
  latestRuns: HeadGreenRunSummary[];
  blockingRuns: HeadGreenRunSummary[];
  statusState?: string;
};

type ValidateRequestIssueResult = {
  errors: string[];
  errorsGrouped?: unknown;
  errorsFormatted: string;
  errorsFormattedSingle: string;
  validationIssues?: { message: string; path: string }[];
  formData?: FormData;
  template?: TemplateLike;
  namespace: string;
  nsType: string;
};

type MergeMethod = 'merge' | 'squash' | 'rebase';

type EffectiveConstants = {
  globalLabels: string[];
  reviewRequestedLabels: string[];
  labelOnApproved: string | null;
  labelAutoMergeCandidate: string | null;
  approverUsernames: string[];
  approverPoolUsernames: string[];
};

const log = infraLog;

function github<E extends RequestEvents>(context: BotContext<E>): GitHubGateway {
  return createGitHubGateway(context);
}

function toStringTrim(value: unknown): string {
  return toStringTrimPure(value);
}

function normalizeLogin(value: unknown): string {
  return normalizeLoginPure(value);
}

function uniqLogins(values: string[]): string[] {
  return uniqLoginsPure(values);
}

function readCheckRunId(run: CheckRunLike | null): number | null {
  const id = run?.id;
  return typeof id === 'number' && Number.isFinite(id) ? id : null;
}

type CheckSuitePullRequestRef = { number?: number | null };

type CheckSuiteLike = {
  id?: number | null;
  status?: string | null;
  conclusion?: string | null;
  head_sha?: string | null;
  head_branch?: string | null;
  pull_requests?: CheckSuitePullRequestRef[] | null;
};

function readCheckSuiteFromPayload(payload: unknown): CheckSuiteLike | null {
  return readCheckSuiteFromPayloadApplication<CheckSuiteLike>(payload, buildCheckPrResolutionCallbacks());
}

function readCheckSuiteId(suite: CheckSuiteLike | null): number | null {
  return readCheckSuiteIdApplication(suite);
}

async function resolveCheckSuitePrNumbers(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  suite: CheckSuiteLike | null,
  headSha: string
): Promise<number[]> {
  return await resolveCheckSuitePrNumbersApplication(
    context,
    repoInfo,
    suite,
    headSha,
    buildCheckPrResolutionCallbacks()
  );
}

async function listAllCheckRunsForSuite(
  context: BotContext<RequestEvents>,
  owner: string,
  repo: string,
  checkSuiteId: number
): Promise<CheckRunLike[]> {
  return await listAllCheckRunsForSuiteApplication(
    context,
    owner,
    repo,
    checkSuiteId,
    buildCheckSuiteAnnotationsCallbacks()
  );
}

async function readFirstRegistryValidationArtifactsForSuiteRuns(
  context: BotContext<RequestEvents>,
  owner: string,
  repo: string,
  runsForSuite: CheckRunLike[]
): Promise<{
  byFile: Map<string, string[]>;
  machineReadableSources: RegistryValidationMachineReadableSource[];
} | null> {
  return await readFirstRegistryValidationArtifactsForSuiteRunsApplication(
    context,
    owner,
    repo,
    runsForSuite,
    buildCheckSuiteAnnotationsCallbacks()
  );
}

function buildCheckSuiteAnnotationsCallbacks(): CheckSuiteAnnotationsCallbacks<
  BotContext<RequestEvents>,
  CheckRunLike
> {
  return {
    isPlainObject,
    readCheckRunId,
    listCheckRunsForSuite: async (
      context: BotContext<RequestEvents>,
      args: {
        owner: string;
        repo: string;
        check_suite_id: number;
        per_page: number;
        page: number;
      }
    ): Promise<{ data?: unknown }> => await github(context).checks.listCheckRunsForSuite(args),
    listCheckRunAnnotations: async (
      context: BotContext<RequestEvents>,
      args: {
        owner: string;
        repo: string;
        check_run_id: number;
        per_page: number;
        page: number;
      }
    ): Promise<{ data?: unknown }> => await github(context).checks.listCheckRunAnnotations(args),
    onCheckRunAnnotationsLoaded: (
      context: BotContext<RequestEvents>,
      args: {
        checkRunId: number;
        annotationsTotal: number;
        relevant: number;
      }
    ): void => {
      if (DBG) {
        log(
          context,
          'debug',
          { checkRunId: args.checkRunId, annotationsTotal: args.annotationsTotal, relevant: args.relevant },
          'dbg:checks:annotations loaded (suite run)'
        );
      }
    },
  };
}

const normalizeKey = (s: unknown): string => {
  const base = toStringTrim(s).toLowerCase();
  return base.replaceAll(/[^\w]+/g, '-').replaceAll(/(?:^-+|-+$)/g, '');
};

async function buildRegistryValidationAggregatePrCommentBody(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  byFile: Map<string, string[]>,
  machineReadableSources: RegistryValidationMachineReadableSource[]
): Promise<string> {
  return await buildRegistryValidationAggregatePrCommentBodyApplication(
    context,
    repoInfo,
    byFile,
    machineReadableSources,
    buildRequestValidationPostingCallbacks()
  );
}

function buildRequestValidationPostingCallbacks(): RequestValidationPostingCallbacks<
  BotContext<RequestEvents>,
  RepoInfo
> {
  return {
    readRepoFileText,
  };
}

const labelName = (l: unknown): string => {
  if (typeof l === 'string') return l;
  if (isPlainObject(l) && typeof l.name === 'string') return l.name;
  return '';
};

const toLabelNames = (labels: unknown): string[] =>
  (Array.isArray(labels) ? labels : [])
    .map((l) => labelName(l))
    .map((s) => toStringTrim(s))
    .filter(Boolean);

const ISSUE_FORM_FIELD_HEADING_RE = /^###\s+\S+/m;

function hasIssueFormInputs(issue: IssueLike | null | undefined): boolean {
  const body = stripQuoteAndCode(issue?.body);
  return ISSUE_FORM_FIELD_HEADING_RE.test(body);
}

const isBotSender = (sender: SenderLike | undefined | null): boolean =>
  sender?.type === 'Bot' || /(\[bot\]|-bot)$/i.test(sender?.login || '');

const head = (s: unknown): string => toStringTrim(s).split(':')[0].trim();

const resolveEffectiveConstants = (context: BotContext<RequestEvents>): EffectiveConstants => {
  const cfg: NormalizedStaticConfig = context.resourceBotConfig ?? DEFAULT_CONFIG;
  const wf = cfg?.workflow ?? {};
  let labels: Record<string, unknown> = {};
  if (isPlainObject(wf)) {
    const raw = (wf as Record<string, unknown>)['labels'];
    if (isPlainObject(raw)) labels = raw;
  }

  const toStringArray = (raw: unknown): string[] => {
    if (Array.isArray(raw)) return raw.map((x) => toStringTrim(x)).filter(Boolean);
    if (raw !== undefined && raw !== null) return [toStringTrim(raw)].filter(Boolean);
    return [];
  };

  const globalLabels = toStringArray(labels['global']);
  const reviewRequestedLabels = toStringArray(labels['approvalRequested']);
  const approvalSuccessfulArr = toStringArray(labels['approvalSuccessful']);
  const labelOnApproved = approvalSuccessfulArr.length ? approvalSuccessfulArr[0] : null;
  const autoMergeCandidateArr = toStringArray(labels['autoMergeCandidate']);
  const labelAutoMergeCandidate = autoMergeCandidateArr.length ? autoMergeCandidateArr[0] : null;

  let approverUsernames: string[] = [];
  let approverPoolUsernames: string[] = [];

  if (isPlainObject(wf)) {
    const rawApprovers = (wf as Record<string, unknown>)['approvers'];
    if (Array.isArray(rawApprovers)) approverUsernames = rawApprovers.map((x) => toStringTrim(x)).filter(Boolean);

    const rawApproversPool = (wf as Record<string, unknown>)['approversPool'];
    if (Array.isArray(rawApproversPool)) {
      approverPoolUsernames = rawApproversPool.map((x) => toStringTrim(x)).filter(Boolean);
    }
  }

  return {
    globalLabels: globalLabels.map((x) => x.trim()).filter(Boolean),
    reviewRequestedLabels: reviewRequestedLabels.map((x) => x.trim()).filter(Boolean),
    labelOnApproved: labelOnApproved ? String(labelOnApproved).trim() : null,
    labelAutoMergeCandidate: labelAutoMergeCandidate ? String(labelAutoMergeCandidate).trim() : null,
    approverUsernames: uniqLogins(approverUsernames.map((x) => x.trim()).filter(Boolean)),
    approverPoolUsernames: uniqLogins(approverPoolUsernames.map((x) => x.trim()).filter(Boolean)),
  };
};

function resolveLockedWorkflowLabelKeys(context: BotContext<RequestEvents>): Set<string> {
  const cfg: NormalizedStaticConfig = context.resourceBotConfig ?? DEFAULT_CONFIG;
  const wf = cfg?.workflow ?? {};

  let labelsCfg: Record<string, unknown> = {};
  if (isPlainObject(wf)) {
    const raw = (wf as Record<string, unknown>)['labels'];
    if (isPlainObject(raw)) labelsCfg = raw;
  }

  const labels: string[] = [];
  for (const v of Object.values(labelsCfg)) {
    if (Array.isArray(v)) labels.push(...v.map((x) => toStringTrim(x)).filter(Boolean));
    else labels.push(toStringTrim(v));
  }

  return new Set(labels.map(normalizeKey).filter(Boolean));
}

function resolveApproverRoutingForRequestType(
  context: BotContext<RequestEvents>,
  requestType: string | undefined | null,
  fallbackApprovers: string[],
  fallbackApproversPool: string[]
): {
  approvalUsernames: string[];
  autoAssigneePoolUsernames: string[];
} {
  const fallbackApprovalUsernames = uniqLogins([...(fallbackApprovers || []), ...(fallbackApproversPool || [])]);
  const fallbackPoolUsernames = uniqLogins(fallbackApproversPool || []);

  const rt = toStringTrim(requestType);
  if (!rt) {
    return {
      approvalUsernames: fallbackApprovalUsernames,
      autoAssigneePoolUsernames: fallbackPoolUsernames,
    };
  }

  const cfg: NormalizedStaticConfig = context.resourceBotConfig ?? DEFAULT_CONFIG;
  const reqs = cfg?.requests;

  if (!reqs || typeof reqs !== 'object') {
    return {
      approvalUsernames: fallbackApprovalUsernames,
      autoAssigneePoolUsernames: fallbackPoolUsernames,
    };
  }

  const asRec = reqs as unknown as Record<string, unknown>;
  const direct = asRec[rt];

  let entry: Record<string, unknown> | null = null;

  if (isPlainObject(direct)) {
    entry = direct;
  } else {
    const rtKey = normalizeKey(rt);
    for (const [k, v] of Object.entries(asRec)) {
      if (normalizeKey(k) === rtKey && isPlainObject(v)) {
        entry = v;
        break;
      }
    }
  }

  if (!entry) {
    return {
      approvalUsernames: fallbackApprovalUsernames,
      autoAssigneePoolUsernames: fallbackPoolUsernames,
    };
  }

  const hasOwnApprovers = Array.isArray(entry['approvers']);
  const hasOwnApproversPool = Array.isArray(entry['approversPool']);

  if (!hasOwnApprovers && !hasOwnApproversPool) {
    return {
      approvalUsernames: fallbackApprovalUsernames,
      autoAssigneePoolUsernames: fallbackPoolUsernames,
    };
  }

  const ownApprovers = hasOwnApprovers
    ? (entry['approvers'] as unknown[]).map((x) => toStringTrim(x)).filter(Boolean)
    : [];

  const ownApproversPool = hasOwnApproversPool
    ? (entry['approversPool'] as unknown[]).map((x) => toStringTrim(x)).filter(Boolean)
    : [];

  return {
    approvalUsernames: uniqLogins([...ownApprovers, ...ownApproversPool]),
    autoAssigneePoolUsernames: uniqLogins(ownApproversPool),
  };
}

function resolveApproversForRequestType(
  context: BotContext<RequestEvents>,
  requestType: string | undefined | null,
  fallbackApprovers: string[],
  fallbackApproversPool: string[] = []
): string[] {
  return resolveApproverRoutingForRequestType(context, requestType, fallbackApprovers, fallbackApproversPool)
    .approvalUsernames;
}

function pickAutoAssigneeFromPool(issue: IssueLike, approversPool: string[]): string[] {
  const users = uniqLogins(approversPool || []).sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));
  if (!users.length) return [];

  const issueNumber = typeof issue?.number === 'number' && Number.isFinite(issue.number) ? issue.number : 1;
  const idx = (Math.max(issueNumber, 1) - 1) % users.length;

  return [users[idx]];
}

const buildTemplateLoadErrorMessage = (errMsg: unknown): string => {
  const msg = toStringTrim(errMsg);
  const isRouting = msg.includes('no routing label found') || msg.includes('Cannot resolve template');

  if (!isRouting) {
    return `## Configuration error: unable to load request template\n\n**Details**\n- ${msg || 'Unknown error'}`;
  }

  return `## Cannot process this issue: no routing label detected

This bot routes request issues by a **unique label** that is auto-assigned by the selected Issue Form template.

**Fix**
- Ensure the Issue Form template includes a unique routing label
- Ensure this label exists in the repo (Settings → Issues → Labels)
- Re-open or edit the issue to retrigger

**Details**
- ${msg || 'No routing label found'}`;
};

function isRequestIssue(
  context: BotContext<RequestEvents>,
  template: TemplateLike | null | undefined,
  parsedFormData: FormData
): boolean {
  const parsedKeys = Object.keys(parsedFormData || {}).filter(Boolean);
  const meta = template?._meta || {};
  const requestType = String(meta.requestType || '').trim();
  const root = String(meta.root || '').trim();
  const schema = String(meta.schema || '').trim();

  const hasTemplateMeta = Boolean(requestType && root && schema);
  const hasFormData = parsedKeys.length > 0;

  const isReq = Boolean(template) && hasTemplateMeta && hasFormData;

  if (DBG) {
    log(
      context,
      'debug',
      {
        tplPath: String(meta.path || '').trim(),
        requestType,
        root,
        schema,
        parsedKeys,
        isReq,
      },
      'isRequestIssue(new-requests-only)'
    );
  }

  return isReq;
}

// Typed wrappers around JS modules
type SetStateLabelFn = (
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  state: 'author' | 'review'
) => Promise<void>;

type PostOnceFn = (
  context: BotContext<RequestEvents>,
  params: IssueParams,
  body: string,
  options?: PostOnceOptions
) => Promise<void>;

type CollapseBotCommentsByPrefixFn = (
  context: BotContext<RequestEvents>,
  params: IssueParams,
  options: CollapseBotCommentsByPrefixOptions
) => Promise<void>;

type LoadTemplateFn = (
  context: BotContext<RequestEvents>,
  opts: {
    owner: string;
    repo: string;
    templatePath?: string;
    issueLabels?: unknown;
    issueTitle?: string;
  }
) => Promise<TemplateLike>;

type ParseFormFn = (body: string, template: TemplateLike) => FormData;

type ValidateRequestIssueFn = (
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  options?: { template?: TemplateLike; formData?: FormData }
) => Promise<ValidateRequestIssueResult>;

type ApprovalHandlingResult = 'approved' | 'rejected' | 'continue';

type RunApprovalHookFn = (
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  args: {
    requestType: string;
    namespace?: string | null;
    resourceName?: string | null;
    formData: FormData;
    issue: IssueLike;
    requestAuthorId?: string | null;
  }
) => Promise<ApprovalDecision | boolean>;

type CalcSnapshotHashFn = (formData: FormData, template: TemplateLike, rawBody: string) => string;

type ExtractHashFromPrBodyFn = (body: string) => string;

type FindOpenIssuePrsFn = (
  context: BotContext<RequestEvents>,
  repo: RepoInfo,
  issueNumber: number
) => Promise<PullRequestLike[]>;

type CreateRequestPrFn = (
  context: BotContext<RequestEvents>,
  repo: RepoInfo,
  issue: IssueLike,
  formData: FormData,
  options?: { template?: TemplateLike }
) => Promise<{ number: number }>;

type TryMergeIfGreenFn = (
  context: BotContext<RequestEvents>,
  args: {
    owner: string;
    repo: string;
    prNumber: number;
    mergeMethod: MergeMethod;
    prData: PullRequestLike;
  }
) => Promise<boolean | void>;

const setStateLabel = setStateLabelRaw as unknown as SetStateLabelFn;
const postOnce = postOnceRaw as unknown as PostOnceFn;
const collapseBotCommentsByPrefix = collapseBotCommentsByPrefixRaw as unknown as CollapseBotCommentsByPrefixFn;
const loadTemplate = loadTemplateRaw as unknown as LoadTemplateFn;
const parseForm = parseFormRaw as unknown as ParseFormFn;
const validateRequestIssue = validateRequestIssueRaw as unknown as ValidateRequestIssueFn;
const runApprovalHook = runApprovalHookRaw as unknown as RunApprovalHookFn;
const calcSnapshotHash = calcSnapshotHashRaw as unknown as CalcSnapshotHashFn;
const extractHashFromPrBody = extractHashFromPRBodyRaw as unknown as ExtractHashFromPrBodyFn;
const findOpenIssuePrs = findOpenIssuePRsRaw as unknown as FindOpenIssuePrsFn;
const createRequestPr = createRequestPRRaw as unknown as CreateRequestPrFn;
const tryMergeIfGreen = tryMergeIfGreenRaw as unknown as TryMergeIfGreenFn;

function readCheckRunFromPayload(payload: unknown): CheckRunLike | null {
  return readCheckRunFromPayloadApplication<CheckRunLike>(payload, buildCheckPrResolutionCallbacks());
}

function readCheckRunPrNumbers(run: CheckRunLike | null): number[] {
  return readCheckRunPrNumbersApplication(run);
}

function buildCheckPrResolutionCallbacks(): CheckPrResolutionCallbacks<BotContext<RequestEvents>, PullRequestLike> {
  return {
    isPlainObject,
    listPullRequestsAssociatedWithCommit: async (
      context: BotContext<RequestEvents>,
      args: {
        owner: string;
        repo: string;
        commit_sha: string;
        per_page: number;
      }
    ): Promise<{ data?: unknown }> => await context.octokit.repos.listPullRequestsAssociatedWithCommit(args),
    listPulls: async (
      context: BotContext<RequestEvents>,
      args: {
        owner: string;
        repo: string;
        state: 'open';
        per_page: number;
        page: number;
      }
    ): Promise<{ data?: PullRequestLike[] }> =>
      (await github(context).pullRequests.listPullRequests(args)) as { data?: PullRequestLike[] },
  };
}

function buildCheckSuiteCiCommentingCallbacks(): CheckSuiteCiCommentingCallbacks<
  BotContext<RequestEvents>,
  RepoInfo,
  RegistryValidationMachineReadableSource
> {
  return {
    collapseBotCommentsByPrefix,
    buildRegistryValidationAggregatePrCommentBody,
    postOnce,
    onBeforePost: (
      context: BotContext<RequestEvents>,
      args: { prNumber: number; files: string[]; bodyLength: number }
    ): void => {
      if (DBG) {
        log(
          context,
          'debug',
          { prNumber: args.prNumber, files: args.files, bodyLen: args.bodyLength },
          'dbg:checks:posting PR comment'
        );
      }
    },
  };
}

async function postCheckSuiteRegistryValidationComments(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumbers: number[],
  artifacts: {
    byFile: Map<string, string[]>;
    machineReadableSources: RegistryValidationMachineReadableSource[];
  },
  minimizeTag: string
): Promise<void> {
  await postCheckSuiteRegistryValidationCommentsApplication(
    context,
    repoInfo,
    prNumbers,
    artifacts,
    minimizeTag,
    buildCheckSuiteCiCommentingCallbacks()
  );
}

function extractResourceNameFromForm(formData: FormData, template: TemplateLike): string {
  const rt = toStringTrim(template?._meta?.requestType).toLowerCase();
  const isProduct = rt === 'product';

  const val = isProduct
    ? (formData['product-id'] ?? formData['productId'] ?? formData['identifier'] ?? formData['id'] ?? '')
    : (formData['identifier'] ??
      formData['namespace'] ??
      formData['id'] ??
      formData['name'] ??
      formData['vendor'] ??
      '');

  return toStringTrim(val);
}

function resolveEffectiveRequestType(template: TemplateLike, formData: FormData): string {
  const rt = toStringTrim(template?._meta?.requestType);

  if (rt && rt.toLowerCase() === 'partnernamespace') {
    const selected = toStringTrim((formData as Record<string, unknown>)['requestType']);
    const norm = selected.replace(/[\s_-]/g, '').toLowerCase();

    if (norm === 'authority') return 'authorityNamespace';
    if (norm === 'system') return 'systemNamespace';
    if (norm === 'subcontext') return 'subContextNamespace';
  }

  return rt;
}

async function fetchIssueLabels(
  context: BotContext<RequestEvents>,
  { owner, repo, issue_number }: IssueParams
): Promise<string[]> {
  return await fetchIssueLabelsApplication(
    context,
    { owner, repo, issue_number },
    buildIssueStateReviewerOperationsCallbacks()
  );
}

async function ensureLabelsPresentOnce(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  labels: string[]
): Promise<void> {
  await ensureLabelsPresentOnceApplication(context, params, labels, buildIssueStateReviewerOperationsCallbacks());
}

async function ensureAssigneesPresent(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  assignees: string[]
): Promise<void> {
  await ensureAssigneesPresentApplication(context, params, assignees, buildIssueStateReviewerOperationsCallbacks());
}

function buildReviewHandoverBody(
  context: BotContext<RequestEvents>,
  snapshotHash?: string,
  options: { target?: 'issue' | 'pull_request' } = {}
): string {
  const docsLinks = getDocLinksFromConfig(context.resourceBotConfig ?? DEFAULT_CONFIG);
  return buildReviewHandoverBodyPure(docsLinks, snapshotHash, options);
}

async function ensureReviewLabelsPresentOnIssue(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  eff: EffectiveConstants
): Promise<boolean> {
  return await ensureReviewLabelsPresentOnIssueApplication(
    context,
    params,
    issue,
    eff,
    buildIssueStateReviewerOperationsCallbacks()
  );
}

function resolveWorkflowLabel(context: BotContext<RequestEvents>, key: string, fallback: string): string {
  const cfg: NormalizedStaticConfig = context.resourceBotConfig ?? DEFAULT_CONFIG;
  const wf = cfg?.workflow ?? {};

  if (!isPlainObject(wf)) return fallback;

  const labelsCfg = isPlainObject((wf as Record<string, unknown>)['labels'])
    ? ((wf as Record<string, unknown>)['labels'] as Record<string, unknown>)
    : {};

  const raw = labelsCfg[key];

  if (Array.isArray(raw)) {
    return toStringTrim(raw[0]) || fallback;
  }

  return toStringTrim(raw) || fallback;
}

const labelsMatching = (labels: string[], expected: string): string[] => {
  const expectedKey = normalizeKey(expected);
  if (!expectedKey) return [];

  return (labels || []).filter((l) => {
    const k = normalizeKey(l);
    return k === expectedKey || k.includes(expectedKey) || expectedKey.includes(k);
  });
};

async function clearParentOwnerActionState(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  currentLabels?: string[]
): Promise<void> {
  await clearParentOwnerActionStateApplication(
    context,
    params,
    buildOwnerApprovalRequirementsCallbacks(),
    currentLabels
  );
}

async function setParentOwnerActionState(context: BotContext<RequestEvents>, params: IssueParams): Promise<void> {
  await setParentOwnerActionStateApplication(context, params, buildOwnerApprovalRequirementsCallbacks());
}

async function assignParentOwnersForApproval(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  owners: string[]
): Promise<void> {
  await assignParentOwnersForApprovalApplication(context, params, owners, buildOwnerApprovalRequirementsCallbacks());
}

async function removeExactLabelsFromIssue(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  labelsToRemove: string[]
): Promise<void> {
  await removeExactLabelsFromIssueApplication(
    context,
    params,
    labelsToRemove,
    buildIssueStateReviewerOperationsCallbacks()
  );
}

async function removeProgressStatusLabels(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  currentLabels?: string[]
): Promise<void> {
  await removeProgressStatusLabelsApplication(
    context,
    params,
    currentLabels,
    buildIssueStateReviewerOperationsCallbacks()
  );
}

async function removeRejectedStatusLabel(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  currentLabels?: string[]
): Promise<void> {
  await removeRejectedStatusLabelApplication(
    context,
    params,
    currentLabels,
    buildIssueStateReviewerOperationsCallbacks()
  );
}

// Higher-level orchestration helpers to reduce handler complexity
function isAuthorizedApprover(
  commenter: string,
  issueAuthor: string | undefined | null,
  allowedApprovers: string[]
): boolean {
  return isAuthorizedApproverPure(commenter, issueAuthor, allowedApprovers);
}

async function applyApprovedRequestState(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  eff: EffectiveConstants
): Promise<void> {
  await applyApprovedRequestStateApplication(context, params, eff, buildIssueStateReviewerOperationsCallbacks());
}

function prAsIssueLike(pr: PullRequestLike): IssueLike {
  return {
    number: pr.number,
    title: pr.title,
    body: pr.body,
    state: pr.state,
    user: pr.user,
    labels: [],
  };
}

function resolveReviewAssigneesForRequestTypes(
  context: BotContext<RequestEvents>,
  reviewTarget: IssueLike,
  requestTypes: string[]
): string[] {
  const eff = resolveEffectiveConstants(context);
  const types = Array.from(new Set((requestTypes || []).map(toStringTrim).filter(Boolean)));

  const pickFromPoolOnly = (pool: string[]): string[] => {
    const normalizedPool = uniqLogins((pool || []).map(toStringTrim).filter(Boolean));
    return normalizedPool.length ? pickAutoAssigneeFromPool(reviewTarget, normalizedPool) : [];
  };

  // Standalone direct PR with unresolved request type:
  // assign from workflow approversPool only. Do not assign all workflow approvers.
  if (!types.length) {
    return pickFromPoolOnly(eff.approverPoolUsernames);
  }

  const assignees: string[] = [];

  for (const requestType of types) {
    const routing = resolveApproverRoutingForRequestType(
      context,
      requestType,
      eff.approverUsernames,
      eff.approverPoolUsernames
    );

    // Important:
    // For standalone direct PR assignment, only approversPool is used.
    // approvalUsernames remain allowed to approve, but are not all assigned.
    assignees.push(...pickFromPoolOnly(routing.autoAssigneePoolUsernames));
  }

  return uniqLogins(assignees);
}

function resolveAllowedApproversForRequestTypes(context: BotContext<RequestEvents>, requestTypes: string[]): string[] {
  return resolveAllowedApproversForRequestTypesApplication(
    context,
    requestTypes,
    buildDirectPrApproverResolutionCallbacks()
  );
}

function buildDirectPrApproverResolutionCallbacks(): DirectPrApproverResolutionCallbacks<BotContext<RequestEvents>> {
  return {
    resolveEffectiveConstants,
    resolveApproverRoutingForRequestType,
    uniqLogins,
    toStringTrim,
  };
}

function calcStandaloneDirectPrSnapshotHash(pr: PullRequestLike, changedFiles: string[]): string {
  const payload = {
    headSha: toStringTrim(pr.head?.sha),
    files: Array.from(new Set((changedFiles || []).map(normalizeRepoPath).filter(Boolean))).sort(),
  };

  return createHash('sha1').update(JSON.stringify(payload)).digest('hex');
}

async function resolvePullRequestRequestAuthorId(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike
): Promise<string> {
  return await resolvePullRequestRequestAuthorIdApplication(
    context,
    repoInfo,
    pr,
    buildPullRequestAuthorResolutionCallbacks()
  );
}

function buildPullRequestAuthorResolutionCallbacks(): PullRequestAuthorResolutionCallbacks {
  return {
    normalizeLogin,
  };
}

async function addApprovedLabelToPr(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number,
  options: { skipStateCleanup?: boolean } = {}
): Promise<void> {
  await addApprovedLabelToPrApplication(
    context,
    repoInfo,
    prNumber,
    options,
    buildIssueStateReviewerOperationsCallbacks()
  );
}

function buildAutoApprovalReviewMarker(headSha: string): string {
  return buildAutoApprovalReviewMarkerPure(headSha);
}

function buildAutomatedApprovalReviewCallbacks(): AutomatedApprovalReviewCallbacks<
  BotContext<RequestEvents>,
  RepoInfo
> {
  return {
    toStringTrim,
    isPlainObject,
    getVisibleApprovalText,
    hasAutoApprovedPrHead,
    hasAutoApprovalReviewForHead,
    markAutoApprovedPrHead,
    addApprovedLabelToPr,
    autoApprovedPrHeadKey,
    logCreated: (context: BotContext<RequestEvents>, prNumber: number, headSha: string): void => {
      log(
        context,
        'info',
        {
          prNumber,
          headSha,
        },
        'automated PR approval review created'
      );
    },
    logCreateFailed: (
      context: BotContext<RequestEvents>,
      prNumber: number,
      status: number | undefined,
      message: string,
      responseData: unknown
    ): void => {
      log(
        context,
        'warn',
        {
          prNumber,
          status,
          message,
          responseData,
        },
        'failed to create automated PR approval review'
      );
    },
    logDedupedInFlight: (context: BotContext<RequestEvents>, prNumber: number, headSha: string): void => {
      log(
        context,
        'info',
        {
          prNumber,
          headSha,
        },
        'automated PR approval review deduped: already in flight'
      );
    },
  };
}

async function ensureAutomatedApprovalReviewForCurrentHead(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  decision: ApprovalDecision,
  options: AutomatedApprovalReviewOptions = {}
): Promise<boolean> {
  return await ensureAutomatedApprovalReviewForCurrentHeadApplication(
    context,
    repoInfo,
    pr,
    decision,
    options,
    buildAutomatedApprovalReviewCallbacks()
  );
}

async function listPullRequestReviews(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number
): Promise<PullRequestReviewLike[]> {
  return await listPullRequestReviewsApplication(context, repoInfo, prNumber);
}

async function hasApprovedLabelOnPr(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number
): Promise<boolean> {
  const eff = resolveEffectiveConstants(context);
  const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';
  if (!approvedLabel) return false;

  try {
    const labels = await fetchIssueLabels(context, {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      issue_number: prNumber,
    });

    return labelsMatching(labels, approvedLabel).length > 0;
  } catch {
    return false;
  }
}

async function isPullRequestApprovedForBranchMaintenance(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  options: { allowLabelFallback?: boolean } = {}
): Promise<boolean> {
  return await isPullRequestApprovedForBranchMaintenanceApplication(
    context,
    repoInfo,
    pr,
    options,
    buildBranchMaintenanceApprovalCallbacks()
  );
}

function buildBranchMaintenanceApprovalCallbacks(): BranchMaintenanceApprovalCallbacks<
  BotContext<RequestEvents>,
  PullRequestLike
> {
  return {
    hasApprovedLabelOnPr,
    isSnapshotManagedRequestPr,
  };
}

async function hasAutoApprovalReviewForHead(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number,
  headSha: string
): Promise<boolean> {
  return await hasAutoApprovalReviewForHeadApplication(
    context,
    repoInfo,
    prNumber,
    headSha,
    buildAutoApprovalReviewDetectionCallbacks()
  );
}

function buildAutoApprovalReviewDetectionCallbacks(): AutoApprovalReviewDetectionCallbacks<BotContext<RequestEvents>> {
  return {
    buildAutoApprovalReviewMarker,
    listPullRequestReviews,
    toStringTrim,
  };
}

async function evaluateHeadGreenForApprovalReevaluation(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  headSha: string
): Promise<HeadGreenEvaluation> {
  return await evaluateHeadGreenForApprovalReevaluationApplication(
    context,
    repoInfo,
    headSha,
    buildHeadGreenEvaluationCallbacks()
  );
}

function buildHeadGreenEvaluationCallbacks(): HeadGreenEvaluationCallbacks<BotContext<RequestEvents>> {
  return {
    isPlainObject,
    getErrorMessage,
    getHttpStatus,
    logCheckRunsFetchFailed: (
      context: BotContext<RequestEvents>,
      args: { repoInfo: RepoInfo; headSha: string; error: unknown }
    ): void => {
      log(
        context,
        'warn',
        {
          owner: args.repoInfo.owner,
          repo: args.repoInfo.repo,
          headSha: args.headSha,
          err: getErrorMessage(args.error),
          status: getHttpStatus(args.error),
        },
        'head-green:check-runs-fetch-failed'
      );
    },
  };
}

function autoApprovedPrHeadKey(repoInfo: RepoInfo, prNumber: number, headSha: string): string {
  return autoApprovedPrHeadKeyApplication(repoInfo, prNumber, toStringTrim(headSha));
}

function markAutoApprovedPrHead(repoInfo: RepoInfo, prNumber: number, headSha: string): void {
  markAutoApprovedPrHeadApplication(repoInfo, prNumber, toStringTrim(headSha));
}

function hasAutoApprovedPrHead(repoInfo: RepoInfo, prNumber: number, headSha: string): boolean {
  return hasAutoApprovedPrHeadApplication(repoInfo, prNumber, toStringTrim(headSha));
}

const DEFAULT_BRANCH_UPDATE_RETRY_DELAY_MS = 5000;
const UPDATE_BRANCH_RETRY_DELAY_MS = 2000;

type SequentialRegistryPrResult = {
  updated: boolean;
  processed: boolean;
  blockedByActive: boolean;
};

function updateBranchInflightKey(repoInfo: RepoInfo, pr: PullRequestLike): string {
  return `${repoInfo.owner}/${repoInfo.repo}#${pr.number}`;
}

async function callPullRequestBranchUpdate(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number,
  expectedHeadSha?: string
): Promise<void> {
  await callPullRequestBranchUpdateApplication(context, repoInfo, prNumber, expectedHeadSha);
}

async function runBranchUpdateBenignFailureRetry(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number,
  headSha: string
): Promise<BranchUpdateBenignRetryOutcome> {
  return await runBranchUpdateBenignFailureRetryApplication(
    context,
    repoInfo,
    prNumber,
    headSha,
    UPDATE_BRANCH_RETRY_DELAY_MS,
    buildBranchUpdateBenignRetryCallbacks()
  );
}

function buildBranchUpdateBenignRetryCallbacks(): BranchUpdateBenignRetryCallbacks<
  BotContext<RequestEvents>,
  PullRequestLike
> {
  return {
    readFreshPullRequest,
    readMergeableState,
    isPullRequestBehindBase,
    delayMs,
    callPullRequestBranchUpdate,
    getHttpStatus,
    getErrorMessage,
  };
}

async function requestPullRequestBranchUpdate(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  reason: string
): Promise<boolean> {
  return await requestPullRequestBranchUpdateApplication(
    context,
    repoInfo,
    pr,
    reason,
    buildBranchUpdateOrchestrationCallbacks()
  );
}

function buildBranchUpdateOrchestrationCallbacks(): BranchUpdateOrchestrationCallbacks<
  BotContext<RequestEvents>,
  RepoInfo,
  PullRequestLike
> {
  return {
    updateBranchInflightKey,
    runBranchUpdateBenignFailureRetry,
    getErrorMessage,
    getHttpStatus,
    log,
  };
}

function delayMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildBranchUpdateDecisionCallbacks(): BranchUpdateDecisionCallbacks<BotContext<RequestEvents>> {
  return {
    getBranch: async (
      context: BotContext<RequestEvents>,
      args: { owner: string; repo: string; branch: string }
    ): Promise<{ data?: { commit?: { sha?: string | null } } }> => await github(context).repos.getBranch(args),
    compareCommitsWithBasehead: async (
      context: BotContext<RequestEvents>,
      args: { owner: string; repo: string; basehead: string }
    ): Promise<{ data?: { status?: string | null; ahead_by?: number | null } }> =>
      await (
        context.octokit.repos as unknown as {
          compareCommitsWithBasehead: (args: {
            owner: string;
            repo: string;
            basehead: string;
          }) => Promise<{ data?: { status?: string | null; ahead_by?: number | null } }>;
        }
      ).compareCommitsWithBasehead(args),
    log,
    getErrorMessage,
    getHttpStatus,
  };
}

async function readFreshPullRequest(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number
): Promise<PullRequestLike | null> {
  try {
    const res = (await github(context).pullRequests.getPullRequest({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      pull_number: prNumber,
    })) as { data?: PullRequestLike };

    return res.data || null;
  } catch (error: unknown) {
    log(
      context,
      'warn',
      {
        prNumber,
        err: getErrorMessage(error),
        status: getHttpStatus(error),
      },
      'failed to refresh pull request'
    );

    return null;
  }
}

function readMergeableState(pr: PullRequestLike | null | undefined): string {
  return readMergeableStatePure(pr);
}

function isPullRequestOpen(pr: PullRequestLike | null | undefined): boolean {
  return isPullRequestOpenPure(pr);
}

function isPullRequestBehindBase(pr: PullRequestLike | null | undefined): boolean {
  return isPullRequestBehindBasePure(pr);
}

function isPullRequestDirty(pr: PullRequestLike | null | undefined): boolean {
  return isPullRequestDirtyPure(pr);
}

async function waitForPullRequestMergeability(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  reason: string
): Promise<PullRequestLike> {
  return await waitForPullRequestMergeabilityApplication(
    context,
    repoInfo,
    pr,
    reason,
    buildPullRequestMergeabilityCallbacks()
  );
}

function buildPullRequestMergeabilityCallbacks(): PullRequestMergeabilityCallbacks<
  BotContext<RequestEvents>,
  PullRequestLike
> {
  return {
    readFreshPullRequest,
    delayMs,
    logMergeabilityState: (
      context: BotContext<RequestEvents>,
      args: {
        prNumber: number;
        attempt: number;
        headSha: string;
        mergeable: boolean | null | undefined;
        mergeableState: string;
        reason: string;
      }
    ): void => {
      log(
        context,
        DBG ? 'debug' : 'info',
        {
          prNumber: args.prNumber,
          attempt: args.attempt,
          headSha: args.headSha,
          mergeable: args.mergeable,
          mergeableState: args.mergeableState,
          reason: args.reason,
        },
        'pull-request mergeability state'
      );
    },
  };
}

function parsePositiveIssueNumber(value: string | undefined): number | null {
  if (!value) return null;

  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function parseIssueNumberFromText(value: unknown, patterns: RegExp[]): number | null {
  const raw = toStringTrim(value);
  if (!raw) return null;

  for (const pattern of patterns) {
    const match = pattern.exec(raw);
    const parsed = parsePositiveIssueNumber(match?.[1]);
    if (parsed !== null) return parsed;
  }

  return null;
}

function parseLinkedIssueNumberFromPrBody(body: unknown): number | null {
  return parseIssueNumberFromText(body, [
    /<!--\s*nsreq:issue:(\d+)\s*-->/i,
    /\bsource\s*:\s*#(\d+)\b/i,
    /\bissue\s*#\s*(\d+)\b/i,
    /\bissue\s+(\d+)\b/i,
    /\b(?:fix|fixes|fixed|close|closes|closed|resolve|resolves|resolved)\s*:?\s*#(\d+)\b/i,
  ]);
}

function parseLinkedIssueNumberFromPr(pr: PullRequestLike, repoInfo?: RepoInfo): number | null {
  const fromBody = parseLinkedIssueNumberFromPrBody(pr.body);
  if (fromBody !== null) return fromBody;

  const fromTitle = parseIssueNumberFromText(pr.title, [
    /\bissue\s*#?\s*(\d+)\b/i,
    /\b(?:fix|fixes|fixed|close|closes|closed|resolve|resolves|resolved)\s*:?\s*#(\d+)\b/i,
  ]);

  if (fromTitle !== null) return fromTitle;

  if (repoInfo && isCrossRepositoryPullRequest(pr, repoInfo)) return null;

  return parseIssueNumberFromText(pr.head?.ref, [/(?:^|[-_/])issue[-_/]?(\d+)(?:$|[-_/])/i]);
}

function isSnapshotManagedRequestPr(pr: PullRequestLike): boolean {
  return Boolean(extractHashFromPrBody(toStringTrim(pr.body)));
}

async function listOpenPullRequests(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo
): Promise<PullRequestLike[]> {
  const out: PullRequestLike[] = [];
  let page = 1;

  while (true) {
    const { data } = (await github(context).pullRequests.listPullRequests({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      state: 'open',
      per_page: 100,
      page,
    })) as { data?: PullRequestLike[] };

    const prs = (data || []) as unknown as PullRequestLike[];
    if (!prs.length) break;

    out.push(...prs);

    if (prs.length < 100) break;
    page += 1;
    if (page > 20) break;
  }

  return out;
}

function isApprovalConfigChangePath(filePath: string): boolean {
  return /^\.github\/registry-bot\/config\.(?:[cm]?js|ts|ya?ml)$/i.test(normalizeRepoPath(filePath));
}

function isDefaultBranchPush(payload: unknown): boolean {
  if (!isPlainObject(payload)) return false;

  const ref = toStringTrim(payload['ref']);
  const repoObj = isPlainObject(payload['repository']) ? payload['repository'] : null;
  const defaultBranch = repoObj ? toStringTrim(repoObj['default_branch']) : '';

  return Boolean(ref && defaultBranch && ref === `refs/heads/${defaultBranch}`);
}

function pullRequestTargetsBranch(pr: PullRequestLike, branchName: string): boolean {
  const target = toStringTrim(branchName);
  if (!target) return true;

  const prBase = toStringTrim(pr.base?.ref);
  return !prBase || prBase === target;
}

async function updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
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

function normalizeRepoPath(path: unknown): string {
  return toStringTrim(path)
    .replace(/\\/g, '/')
    .replace(/^\/+/, '')
    .replace(/\/{2,}/g, '/');
}

function isYamlPath(path: string): boolean {
  const p = path.toLowerCase();
  return p.endsWith('.yaml') || p.endsWith('.yml');
}

function normalizeTypeToken(value: unknown): string {
  return toStringTrim(value)
    .replace(/[\s_-]/g, '')
    .toLowerCase();
}

function matchRequestTypesForFile(context: BotContext<RequestEvents>, filePath: string): string[] {
  return matchRequestTypesForFilePure(context.resourceBotConfig ?? DEFAULT_CONFIG, filePath);
}

function pickRequestTypeForChangedResource(
  context: BotContext<RequestEvents>,
  filePath: string,
  doc: Record<string, unknown>
): string {
  return pickRequestTypeForChangedResourcePure(context.resourceBotConfig ?? DEFAULT_CONFIG, filePath, doc);
}

function isRegistryEntryPath(context: BotContext<RequestEvents>, filePath: string): boolean {
  return matchRequestTypesForFile(context, filePath).length > 0;
}
function isChangedYamlCandidate(file: PullRequestFileLike): string {
  return isChangedYamlCandidateApplication(file, {
    normalizeRepoPath,
    isYamlPath,
  });
}

async function listChangedYamlFilesPage(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number,
  page: number
): Promise<PullRequestFileLike[]> {
  return await listChangedYamlFilesPageApplication(context, repoInfo, prNumber, page);
}

async function listChangedYamlFilesForPr(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number
): Promise<string[]> {
  return await listChangedYamlFilesForPrApplication(context, repoInfo, prNumber, {
    listChangedYamlFilesPage,
    isChangedYamlCandidate,
    isRegistryEntryPath,
  });
}

async function listChangedFilesForPr(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number
): Promise<PullRequestFileLike[]> {
  const out: PullRequestFileLike[] = [];
  let page = 1;

  while (true) {
    const files = await listChangedYamlFilesPage(context, repoInfo, prNumber, page);
    if (!files.length) break;

    out.push(...files);

    if (files.length < 100 || page >= 20) break;
    page += 1;
  }

  return out;
}

async function readBranchHeadSha(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  branchName: string
): Promise<string> {
  return await readBranchHeadShaApplication(context, repoInfo, branchName, buildBranchUpdateDecisionCallbacks());
}

async function readRecursiveGitTreeEntries(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  ref: string
): Promise<GitTreeEntryLike[]> {
  return await readRecursiveGitTreeEntriesApplication(context, repoInfo, ref, {
    getErrorMessage,
    getHttpStatus,
    log,
  });
}

function registryYamlTreeEntryPath(context: BotContext<RequestEvents>, entry: GitTreeEntryLike): string {
  return registryYamlTreeEntryPathApplication(context, entry, {
    normalizeRepoPath,
    isYamlPath,
    isRegistryEntryPath,
  });
}

function sameRepoInfo(a: RepoInfo, b: RepoInfo): boolean {
  return sameRepoInfoPure(a, b);
}

function resolvePullRequestHeadRepoInfo(pr: PullRequestLike, fallbackRepoInfo: RepoInfo): RepoInfo {
  return resolvePullRequestHeadRepoInfoPure(pr, fallbackRepoInfo);
}

function isCrossRepositoryPullRequest(pr: PullRequestLike, baseRepoInfo: RepoInfo): boolean {
  return isCrossRepositoryPullRequestPure(pr, baseRepoInfo);
}

function buildPullRequestHeadReadCandidates(
  repoInfo: RepoInfo,
  pr: PullRequestLike
): PullRequestHeadReadCandidate<RepoInfo>[] {
  return buildPullRequestHeadReadCandidatesApplication(repoInfo, pr, {
    resolvePullRequestHeadRepoInfo,
    sameRepoInfo,
  });
}

async function readPullRequestHeadFileText(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  path: string
): Promise<string | null> {
  return await readPullRequestHeadFileTextApplication(context, repoInfo, pr, path, {
    normalizeRepoPath,
    buildPullRequestHeadReadCandidates,
    readRepoFileTextAtRef,
    resolvePullRequestHeadRepoInfo,
    isCrossRepositoryPullRequest,
    log,
  });
}

async function readPullRequestHeadTreeEntries(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike
): Promise<GitTreeEntryLike[]> {
  return await readPullRequestHeadTreeEntriesApplication(context, repoInfo, pr, {
    resolvePullRequestHeadRepoInfo,
    sameRepoInfo,
    readRecursiveGitTreeEntries,
    isCrossRepositoryPullRequest,
    log,
  });
}

async function listChangedYamlFilesForPrAgainstCurrentBase(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch: string
): Promise<string[]> {
  return await listChangedYamlFilesForPrAgainstCurrentBaseApplication(context, repoInfo, pr, baseBranch, {
    readBranchHeadSha,
    readRecursiveGitTreeEntries,
    readPullRequestHeadTreeEntries,
    registryYamlTreeEntryPath,
  });
}

async function listChangedYamlFilesForPrWithFallback(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch?: string
): Promise<string[]> {
  return await listChangedYamlFilesForPrWithFallbackApplication(context, repoInfo, pr, baseBranch, {
    listChangedYamlFilesForPr,
    listChangedYamlFilesForPrAgainstCurrentBase,
    log,
  });
}

function isSafeRegistryWorkflowApprovalFile(context: BotContext<RequestEvents>, file: PullRequestFileLike): boolean {
  const filename = normalizeRepoPath(file?.filename);
  const status = toStringTrim(file?.status).toLowerCase();

  if (!filename || status === 'removed') return false;
  if (!isYamlPath(filename)) return false;
  if (!isRegistryEntryPath(context, filename)) return false;

  return true;
}

function buildWorkflowApprovalCallbacks(): WorkflowApprovalCallbacks<
  BotContext<RequestEvents>,
  RepoInfo,
  PullRequestLike,
  PullRequestFileLike
> {
  return composeWorkflowApprovalCallbacks<BotContext<RequestEvents>, RepoInfo, PullRequestLike, PullRequestFileLike>({
    isPullRequestOpen,
    isSafeRegistryWorkflowApprovalFile,
    listChangedFilesForPr,
    parseLinkedIssueNumberFromPr,
    isSnapshotManagedRequestPr,
    evaluateDirectPrOnApproval,
    hasAllowedStandaloneDirectPrApprovalForCurrentHead,
    readFreshPullRequest,
    isPlainObject,
    log,
    getErrorMessage,
    getHttpStatus,
    toStringTrim,
  });
}

async function maybeApprovePendingWorkflowRunsForRegistryPrWithRetry(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
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
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
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

async function isPullRequestBehindCurrentBase(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch: string
): Promise<boolean> {
  return await isPullRequestBehindCurrentBaseApplication(
    context,
    repoInfo,
    pr,
    baseBranch,
    buildBranchUpdateDecisionCallbacks()
  );
}

async function shouldUpdatePullRequestBranch(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch: string
): Promise<boolean> {
  if (isPullRequestBehindBase(pr)) return true;
  return await isPullRequestBehindCurrentBase(context, repoInfo, pr, baseBranch);
}

function markSequentialRegistryPrHeadSkipped(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  reason: string
): void {
  markSequentialRegistryPrHeadSkippedState(context, repoInfo, pr, reason, {
    log,
  });
}

function markSequentialRegistryPrActive(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  reason: string
): void {
  markSequentialRegistryPrActiveState(context, repoInfo, pr, reason, {
    log,
  });
}

async function isSequentialRegistryPrActiveBlocking(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo
): Promise<boolean> {
  const active = getSequentialRegistryPrActive(repoInfo);
  if (!active) return false;

  if (active.expiresAt <= Date.now()) {
    log(
      context,
      'warn',
      {
        prNumber: active.prNumber,
        startedHeadSha: active.startedHeadSha,
        reason: active.reason,
      },
      'sequential-registry-pr:active-expired'
    );

    clearSequentialRegistryPrActive(repoInfo);
    return false;
  }

  const freshPr = await readFreshPullRequest(context, repoInfo, active.prNumber);

  if (!freshPr || !isPullRequestOpen(freshPr)) {
    log(
      context,
      'info',
      {
        prNumber: active.prNumber,
        startedHeadSha: active.startedHeadSha,
        freshHeadSha: toStringTrim(freshPr?.head?.sha),
        reason: active.reason,
      },
      'sequential-registry-pr:active-cleared-closed'
    );

    clearSequentialRegistryPrActive(repoInfo);
    return false;
  }

  const baseBranch = toStringTrim(freshPr.base?.ref);
  const isDirectRegistryPr = await isSequentialDirectRegistryPr(context, repoInfo, freshPr, baseBranch);

  if (!isDirectRegistryPr) {
    log(
      context,
      'info',
      {
        prNumber: active.prNumber,
        startedHeadSha: active.startedHeadSha,
        currentHeadSha: toStringTrim(freshPr.head?.sha),
        reason: active.reason,
      },
      'sequential-registry-pr:active-cleared-non-direct'
    );

    clearSequentialRegistryPrActive(repoInfo);
    return false;
  }

  log(
    context,
    'info',
    {
      prNumber: active.prNumber,
      startedHeadSha: active.startedHeadSha,
      currentHeadSha: toStringTrim(freshPr.head?.sha),
      reason: active.reason,
    },
    'sequential-registry-pr:active-blocking'
  );

  return true;
}

async function isSequentialDirectRegistryPr(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch?: string
): Promise<boolean> {
  return await isSequentialDirectRegistryPrApplication(context, repoInfo, pr, baseBranch, {
    isSnapshotManagedRequestPr,
    pullRequestTargetsBranch,
    listChangedYamlFilesForPrWithFallback,
    log,
    getErrorMessage,
  });
}

async function readRepoFileTextAtRef(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  path: string,
  ref: string
): Promise<string | null> {
  return await readRepoFileTextAtRefApplication(context, repoInfo, path, ref, {
    normalizeRepoPath,
    isRepoContentFile,
  });
}

async function readRegistryDocForApproval(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  filePath: string
): Promise<Record<string, unknown> | null> {
  return await readRegistryDocForApprovalApplication(context, repoInfo, pr, filePath, {
    readPullRequestHeadFileText,
    isPlainObject,
  });
}

function buildDefaultBranchApprovedPrBranchUpdateCallbacks(): DefaultBranchApprovedPrBranchUpdateCallbacks<
  BotContext<RequestEvents>,
  RepoInfo,
  PullRequestLike
> {
  return composeDefaultBranchApprovedPrBranchUpdateCallbacks<BotContext<RequestEvents>, RepoInfo, PullRequestLike>({
    isSequentialRegistryPrActiveBlocking,
    listOpenPullRequests,
    isSequentialRegistryPrHeadSkipped,
    listChangedYamlFilesForPrWithFallback,
    isSnapshotManagedRequestPr,
    isPullRequestApprovedForBranchMaintenance,
    waitForPullRequestMergeability,
    isPullRequestOpen,
    isPullRequestDirty,
    readMergeableState,
    shouldUpdatePullRequestBranch,
    requestPullRequestBranchUpdate,
    markSequentialRegistryPrHeadSkipped,
    getErrorMessage,
    log,
  });
}

function buildDirectPrChangedResourceApprovalCallbacks(): DirectPrChangedResourceApprovalCallbacks<
  BotContext<RequestEvents>,
  PullRequestLike
> {
  return {
    readRegistryDocForApproval,
    pickRequestTypeForChangedResource,
    runApprovalHook,
    logRegistryDocReadFailed: (
      context: BotContext<RequestEvents>,
      args: { repoInfo: RepoInfo; pr: PullRequestLike; filePath: string }
    ): void => {
      log(
        context,
        'warn',
        {
          prNumber: args.pr.number,
          filePath: args.filePath,
          baseOwner: args.repoInfo.owner,
          baseRepo: args.repoInfo.repo,
          headOwner: resolvePullRequestHeadRepoInfo(args.pr, args.repoInfo).owner,
          headRepo: resolvePullRequestHeadRepoInfo(args.pr, args.repoInfo).repo,
          headRef: toStringTrim(args.pr.head?.ref),
          headSha: toStringTrim(args.pr.head?.sha),
          crossRepo: isCrossRepositoryPullRequest(args.pr, args.repoInfo),
        },
        'direct-pr:on-approval:registry-doc-read-failed'
      );
    },
  };
}

async function evaluateDirectPrOnApproval(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  requestAuthorIdOverride?: string,
  options: DirectPrApprovalOptions = {}
): Promise<ApprovalDecision> {
  return await evaluateDirectPrOnApprovalApplication(
    context,
    repoInfo,
    pr,
    requestAuthorIdOverride,
    options,
    buildDirectPrApprovalEvaluationCallbacks()
  );
}

function buildDirectPrApprovalEvaluationCallbacks(): DirectPrApprovalEvaluationCallbacks<
  BotContext<RequestEvents>,
  PullRequestLike
> {
  return {
    listChangedYamlFilesForPrWithFallback,
    changedResourceApprovalCallbacks: buildDirectPrChangedResourceApprovalCallbacks(),
    logStart: (
      context: BotContext<RequestEvents>,
      args: { repoInfo: RepoInfo; pr: PullRequestLike; requestAuthorId: string; changedFiles: string[] }
    ): void => {
      log(
        context,
        'info',
        {
          prNumber: args.pr.number,
          headSha: toStringTrim(args.pr.head?.sha),
          headRef: toStringTrim(args.pr.head?.ref),
          requestAuthorId: args.requestAuthorId,
          changedFiles: args.changedFiles,
          linkedIssueNumber: parseLinkedIssueNumberFromPr(args.pr, args.repoInfo),
          crossRepo: isCrossRepositoryPullRequest(args.pr, args.repoInfo),
          headOwner: resolvePullRequestHeadRepoInfo(args.pr, args.repoInfo).owner,
          headRepo: resolvePullRequestHeadRepoInfo(args.pr, args.repoInfo).repo,
          hooksSource: context.resourceBotHooksSource,
        },
        'direct-pr:on-approval:start'
      );
    },
    logSkipNoRegistryFiles: (context: BotContext<RequestEvents>, args: { pr: PullRequestLike }): void => {
      log(
        context,
        'info',
        {
          prNumber: args.pr.number,
          headSha: toStringTrim(args.pr.head?.sha),
          headRef: toStringTrim(args.pr.head?.ref),
        },
        'direct-pr:on-approval:skip-no-registry-files'
      );
    },
    logFileDecision: (
      context: BotContext<RequestEvents>,
      args: { pr: PullRequestLike; filePath: string; requestAuthorId: string; decision: ApprovalDecision }
    ): void => {
      log(
        context,
        'info',
        {
          prNumber: args.pr.number,
          filePath: args.filePath,
          requestAuthorId: args.requestAuthorId,
          status: toStringTrim(args.decision.status) || 'none',
          reason: toStringTrim(args.decision.reason),
          message: toStringTrim(args.decision.message),
          path: toStringTrim(args.decision.path),
        },
        'direct-pr:on-approval:file-decision'
      );
    },
  };
}

async function resolveDirectPrRequestTypes(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  options: DirectPrApprovalOptions = {}
): Promise<string[]> {
  return await resolveDirectPrRequestTypesApplication(
    context,
    repoInfo,
    pr,
    options,
    buildDirectPrRequestTypeResolutionCallbacks()
  );
}

function buildDirectPrRequestTypeResolutionCallbacks(): DirectPrRequestTypeResolutionCallbacks<
  BotContext<RequestEvents>,
  PullRequestLike
> {
  return {
    listChangedYamlFilesForPrWithFallback,
    readRegistryDocForApproval,
    pickRequestTypeForChangedResource,
  };
}

async function hasAllowedStandaloneDirectPrApprovalForCurrentHead(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  decision: ApprovalDecision,
  options: DirectPrApprovalOptions = {}
): Promise<boolean> {
  return await hasAllowedStandaloneDirectPrApprovalForCurrentHeadApplication(
    context,
    repoInfo,
    pr,
    decision,
    options,
    buildDirectPrReviewApprovalCallbacks()
  );
}

async function hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  decision: ApprovalDecision,
  options: DirectPrApprovalOptions = {}
): Promise<boolean> {
  return await hasAllowedCurrentHeadManualApprovalForStandaloneDirectPrApplication(
    context,
    repoInfo,
    pr,
    decision,
    options,
    buildDirectPrReviewApprovalCallbacks()
  );
}

function buildDirectPrReviewApprovalCallbacks(): DirectPrReviewApprovalCallbacks<
  BotContext<RequestEvents>,
  PullRequestLike
> {
  return {
    directPrRequestTypeResolutionCallbacks: buildDirectPrRequestTypeResolutionCallbacks(),
    directPrApproverResolutionCallbacks: buildDirectPrApproverResolutionCallbacks(),
    pullRequestAuthorResolutionCallbacks: buildPullRequestAuthorResolutionCallbacks(),
    log: (
      context: BotContext<RequestEvents>,
      level: 'info',
      metadata: Record<string, unknown>,
      message: string
    ): void => {
      log(context, level, metadata, message);
    },
  };
}

function buildStandaloneDirectPrReviewHandoverOptions(): {
  resolveEffectiveConstants: (context: BotContext<RequestEvents>) => EffectiveConstants;
  prAsIssueLike: (pr: PullRequestLike) => IssueLike;
  listChangedYamlFilesForPrWithFallback: (
    context: BotContext<RequestEvents>,
    repoInfo: RepoInfo,
    pr: PullRequestLike,
    baseBranch?: string
  ) => Promise<string[]>;
  resolveDirectPrRequestTypes: (
    context: BotContext<RequestEvents>,
    repoInfo: RepoInfo,
    pr: PullRequestLike,
    options?: DirectPrApprovalOptions
  ) => Promise<string[]>;
  getUnknownManualApprovers: (decision: ApprovalDecision) => string[];
  resolveReviewAssigneesForRequestTypes: (
    context: BotContext<RequestEvents>,
    issue: IssueLike,
    requestTypes: string[]
  ) => string[];
  ensureAssigneesPresent: (
    context: BotContext<RequestEvents>,
    params: IssueParams,
    assignees: string[]
  ) => Promise<void>;
  ensureLabelsPresentOnce: (context: BotContext<RequestEvents>, params: IssueParams, labels: string[]) => Promise<void>;
  calcStandaloneDirectPrSnapshotHash: (pr: PullRequestLike, changedFiles: string[]) => string;
  buildReviewHandoverBody: (
    context: BotContext<RequestEvents>,
    snapshotHash?: string,
    options?: { target?: 'issue' | 'pull_request' }
  ) => string;
  toStringTrim: (value: unknown) => string;
  logHandover: (args: {
    context: BotContext<RequestEvents>;
    prNumber: number;
    requestTypes: string[];
    changedFiles: string[];
    assignees: string[];
    snapshotHash: string;
    decisionStatus: string;
  }) => void;
} {
  return {
    resolveEffectiveConstants,
    prAsIssueLike,
    listChangedYamlFilesForPrWithFallback,
    resolveDirectPrRequestTypes,
    getUnknownManualApprovers,
    resolveReviewAssigneesForRequestTypes,
    ensureAssigneesPresent,
    ensureLabelsPresentOnce,
    calcStandaloneDirectPrSnapshotHash,
    buildReviewHandoverBody,
    toStringTrim,
    logHandover: ({ context, prNumber, requestTypes, changedFiles, assignees, snapshotHash, decisionStatus }): void => {
      log(
        context,
        'info',
        {
          prNumber,
          requestTypes,
          changedFiles,
          assignees,
          snapshotHash,
          decisionStatus,
        },
        'direct-pr:handover-to-review'
      );
    },
  };
}

function buildStandaloneDirectPrApprovalCallbacks(): StandaloneDirectPrApprovalCallbacks<
  BotContext<RequestEvents>,
  RepoInfo,
  PullRequestLike,
  ReturnType<typeof buildStandaloneDirectPrReviewHandoverOptions>
> {
  return composeStandaloneDirectPrApprovalCallbacks<
    BotContext<RequestEvents>,
    RepoInfo,
    PullRequestLike,
    ReturnType<typeof buildStandaloneDirectPrReviewHandoverOptions>
  >({
    evaluateDirectPrOnApproval,
    hasAllowedStandaloneDirectPrApprovalForCurrentHead,
    ensureAutomatedApprovalReviewForCurrentHead,
    postApprovalRejectedOnce,
    hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr,
    addApprovedLabelToPr,
    handoverStandaloneDirectPrToReview,
    isCrossRepositoryPullRequest,
    buildStandaloneDirectPrReviewHandoverOptions,
    log,
  });
}

async function maybeHandleStandaloneDirectPrApproval(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  options: DirectPrApprovalOptions = {}
): Promise<ApprovalHandlingResult> {
  return await maybeHandleStandaloneDirectPrApprovalApplication(
    context,
    repoInfo,
    pr,
    options,
    buildStandaloneDirectPrApprovalCallbacks()
  );
}

const approvalRuntime = createApprovalRuntime<
  BotContext<RequestEvents>,
  RepoInfo,
  IssueParams,
  IssueLike,
  TemplateLike,
  FormData,
  PullRequestLike,
  EffectiveConstants,
  ValidateRequestIssueResult,
  ContactApprovalMeta,
  ParentApprovalMeta
>({
  runApprovalHook,
  extractResourceNameFromForm,
  listOpenPullRequests,
  parseLinkedIssueNumberFromPr,
  rejectRequestFromApprovalHook,
  resolveEffectiveConstants,
  resolveApproverRoutingForRequestType,
  pickAutoAssigneeFromPool,
  uniqLogins,
  toStringTrim,
  ensureAssigneesPresent,
  ensureLabelsPresentOnce,
  buildReviewHandoverBody,
  resolveEffectiveRequestType,
  buildApprovedRequestFinalizationCallbacks,
  buildApprovalCommentHandlingCallbacks,
  buildOwnerApprovalCommentHandlingCallbacks,
  buildOwnerApprovalRequirementsCallbacks,
  buildIssueStateReviewerOperationsCallbacks,
});

const buildApprovalDecisionDispatchOptions = approvalRuntime.buildApprovalDecisionDispatchOptions;
const buildReviewHandoverOptions = approvalRuntime.buildReviewHandoverOptions;
const maybeHandleApprovalDecision = approvalRuntime.maybeHandleApprovalDecision;
const finalizeApprovedRequest = approvalRuntime.finalizeApprovedRequest;
const maybeRequireParentOwnerApproval = approvalRuntime.maybeRequireParentOwnerApproval;
const maybeRequireSystemContactOwnerApproval = approvalRuntime.maybeRequireSystemContactOwnerApproval;
const handleApprovalComment = approvalRuntime.handleApprovalComment;
const handleParentOwnerApprovalIfNeeded = approvalRuntime.handleParentOwnerApprovalIfNeeded;
const handleSystemContactOwnerApprovalIfNeeded = approvalRuntime.handleSystemContactOwnerApprovalIfNeeded;
const resolveManualReviewApproverOverrideFromApprovalHook =
  approvalRuntime.resolveManualReviewApproverOverrideFromApprovalHook;
const resolveAdditionalIssueApproversFromApprovalHook = approvalRuntime.resolveAdditionalIssueApproversFromApprovalHook;

function buildApprovedRequestFinalizationCallbacks(): ApprovedRequestFinalizationCallbacks<
  BotContext<RequestEvents>,
  IssueParams,
  IssueLike,
  TemplateLike,
  FormData,
  EffectiveConstants,
  PullRequestLike
> {
  return composeApprovedRequestFinalizationCallbacks<
    BotContext<RequestEvents>,
    IssueParams,
    IssueLike,
    TemplateLike,
    FormData,
    EffectiveConstants,
    PullRequestLike
  >({
    resolveEffectiveConstants,
    extractResourceNameFromForm,
    resolveEffectiveRequestType,
    resolveAdditionalIssueApproversFromApprovalHook,
    findOpenIssuePrs,
    applyApprovedRequestState,
    addApprovedLabelToPr,
    ensureAssigneesPresent,
    createRequestPrWithRecovery,
    postOnce,
  });
}

function buildDirectPrLinkedIssueApprovalCallbacks(): DirectPrLinkedIssueApprovalCallbacks<
  BotContext<RequestEvents>,
  RepoInfo,
  IssueParams,
  IssueLike,
  PullRequestLike,
  EffectiveConstants
> {
  return {
    resolvePullRequestRequestAuthorId,
    evaluateDirectPrOnApproval,
    ensureAutomatedApprovalReviewForCurrentHead,
    applyApprovedRequestState,
    resolveEffectiveConstants,
    postApprovalRejectedOnce,
    rejectRequestFromApprovalHook: async (
      context: BotContext<RequestEvents>,
      params: IssueParams,
      issue: IssueLike,
      decision: ApprovalDecision
    ): Promise<void> =>
      await rejectRequestFromApprovalHook(context, params, issue, decision, {
        closeLinkedPrs: true,
        minimizeTag: 'nsreq:on-approval:issue-rejected',
        listOpenPullRequests,
        parseLinkedIssueNumberFromPr,
      }),
    postApprovalUnknownOnce,
    log,
  };
}

async function maybeHandleDirectPrApprovalForMerge(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  issueParams: IssueParams,
  issue: IssueLike,
  _template: TemplateLike,
  _parsedFormData: FormData,
  pr: PullRequestLike
): Promise<ApprovalHandlingResult> {
  return await maybeHandleDirectPrApprovalForMergeApplication(
    context,
    repoInfo,
    issueParams,
    issue,
    _template,
    _parsedFormData,
    pr,
    buildDirectPrLinkedIssueApprovalCallbacks()
  );
}

function buildSafeResourceSlug(resourceName: unknown): string {
  return toStringTrim(resourceName)
    .toLowerCase()
    .replace(/[^a-z0-9.-]/g, '-')
    .replace(/-+/g, '-');
}

function renderConfiguredRequestBranchName(
  context: BotContext<RequestEvents>,
  issue: IssueLike,
  resourceName: string
): string {
  const cfg = (context.resourceBotConfig ?? DEFAULT_CONFIG) as unknown as {
    pr?: { branchNameTemplate?: unknown } | null;
  };

  const branchTemplate = toStringTrim(cfg?.pr?.branchNameTemplate) || 'feat/resource-{resource}-issue-{issue}';

  return String(branchTemplate)
    .replace('{resource}', buildSafeResourceSlug(resourceName))
    .replace('{issue}', String(issue.number || ''));
}

async function createRequestPrWithRecovery(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  parsedFormData: FormData,
  template: TemplateLike,
  resourceName: string
): Promise<{ number: number }> {
  return await createRequestPrWithRecoveryApplication(
    context,
    params,
    issue,
    parsedFormData,
    template,
    resourceName,
    buildRequestPrCreationRecoveryCallbacks()
  );
}

function buildRequestPrCreationRecoveryCallbacks(): RequestPrCreationRecoveryCallbacks<
  BotContext<RequestEvents>,
  RepoInfo,
  IssueLike,
  TemplateLike,
  FormData
> {
  return composeRequestPrCreationRecoveryCallbacks<
    BotContext<RequestEvents>,
    RepoInfo,
    IssueLike,
    TemplateLike,
    FormData
  >({
    createRequestPr: async (
      context: BotContext<RequestEvents>,
      repoInfo: RepoInfo,
      issue: IssueLike,
      parsedFormData: FormData,
      options: { template: TemplateLike }
    ): Promise<{ number: number }> => await createRequestPr(context, repoInfo, issue, parsedFormData, options),
    getHttpStatus,
    renderConfiguredRequestBranchName,
  });
}

function isConfiguredApprover(login: string | undefined | null, allowedApprovers: string[]): boolean {
  const who = normalizeLogin(login).toLowerCase();
  if (!who) return false;

  return (allowedApprovers || []).some((u) => normalizeLogin(u).toLowerCase() === who);
}

function buildRequestIssueLifecycleCallbacks(
  app: Probot
): RequestIssueLifecycleCallbacks<
  BotContext<'issues.opened' | 'issues.edited' | 'issues.reopened'>,
  IssueParams,
  IssueLike,
  TemplateLike,
  FormData,
  ValidateRequestIssueResult
> {
  return composeRequestIssueLifecycleCallbacks<
    BotContext<'issues.opened' | 'issues.edited' | 'issues.reopened'>,
    IssueParams,
    IssueLike,
    TemplateLike,
    FormData,
    ValidateRequestIssueResult
  >({
    isJestWorker: Boolean(process.env.JEST_WORKER_ID),
    isDebugEnabled: DBG,
    hasIssueFormInputs,
    loadTemplateWithLabelRefresh,
    buildTemplateLoadErrorMessage,
    postOnce,
    setStateLabel,
    parseForm,
    isRequestIssue,
    log,
    toLabelNames,
    detectSingleRoutingLabel,
    ensureRoutingLockMarker,
    enforceRoutingLabelLock,
    removeRejectedStatusLabel,
    buildCompatibleRequestSnapshotHashes,
    calcSnapshotHash,
    normalizeIssueTitle,
    closeOutdatedRequestPrs,
    validateRequestIssue,
    checkParentChainExistsInFlatStructure,
    resolveEffectiveRequestType,
    maybeRequireParentOwnerApproval,
    maybeRequireSystemContactOwnerApproval,
    getApprovedParentOwnerLogin,
    isSubContextRequestType,
    maybeHandleApprovalDecision: async (
      context,
      params,
      issue,
      template,
      parsedFormData,
      requestType,
      namespace,
      options
    ): Promise<ApprovalHandlingResult> =>
      await maybeHandleApprovalDecision(
        context,
        params,
        issue,
        template,
        parsedFormData,
        requestType,
        namespace,
        options as ReturnType<typeof buildApprovalDecisionDispatchOptions>
      ),
    buildApprovalDecisionDispatchOptions,
    finalizeApprovedRequest,
    resolveManualReviewApproverOverrideFromApprovalHook,
    resolveAdditionalIssueApproversFromApprovalHook,
    handoverToCpa: async (context, params, issue, nsType, namespace, labels, options): Promise<void> =>
      await handoverToCpa(
        context,
        params,
        issue,
        nsType,
        namespace,
        labels,
        options as ReturnType<typeof buildReviewHandoverOptions>
      ),
    buildReviewHandoverOptions: (): Record<string, unknown> =>
      buildReviewHandoverOptions() as unknown as Record<string, unknown>,
    appLog: app.log || console,
  });
}

function buildRequestIssueAuthorUpdateCallbacks(
  app: Probot
): RequestIssueAuthorUpdateCallbacks<
  BotContext<RequestEvents>,
  IssueParams,
  IssueLike,
  TemplateLike,
  FormData,
  ValidateRequestIssueResult
> {
  return composeRequestIssueAuthorUpdateCallbacks<
    BotContext<RequestEvents>,
    IssueParams,
    IssueLike,
    TemplateLike,
    FormData,
    ValidateRequestIssueResult
  >({
    validateRequestIssue,
    parseForm,
    calcSnapshotHash,
    checkParentChainExistsInFlatStructure,
    postOnce,
    setStateLabel,
    closeOutdatedRequestPrs,
    resolveEffectiveRequestType,
    maybeRequireParentOwnerApproval,
    log,
    isDebugEnabled: DBG,
    maybeRequireSystemContactOwnerApproval,
    getApprovedParentOwnerLogin,
    isSubContextRequestType,
    maybeHandleApprovalDecision: async (
      context,
      params,
      issue,
      template,
      parsedFormData,
      requestType,
      namespace,
      options
    ): Promise<ApprovalHandlingResult> =>
      await maybeHandleApprovalDecision(
        context,
        params,
        issue,
        template,
        parsedFormData,
        requestType,
        namespace,
        options as ReturnType<typeof buildApprovalDecisionDispatchOptions>
      ),
    buildApprovalDecisionDispatchOptions,
    finalizeApprovedRequest,
    resolveManualReviewApproverOverrideFromApprovalHook,
    resolveAdditionalIssueApproversFromApprovalHook,
    handoverToCpa: async (context, params, issue, nsType, namespace, labels, options): Promise<void> =>
      await handoverToCpa(
        context,
        params,
        issue,
        nsType,
        namespace,
        labels,
        options as ReturnType<typeof buildReviewHandoverOptions>
      ),
    buildReviewHandoverOptions: (): Record<string, unknown> =>
      buildReviewHandoverOptions() as unknown as Record<string, unknown>,
    appLog: app.log || console,
  });
}

async function processIssueEvent(
  app: Probot,
  context: BotContext<'issues.opened' | 'issues.edited' | 'issues.reopened'>,
  params: IssueParams,
  issue: IssueLike
): Promise<void> {
  await processRequestIssueLifecycleApplication(context, params, issue, buildRequestIssueLifecycleCallbacks(app));
}

function buildIssueStateReviewerOperationsCallbacks(): IssueStateReviewerOperationsCallbacks<
  BotContext<RequestEvents>,
  IssueParams,
  IssueLike,
  TemplateLike,
  FormData,
  EffectiveConstants
> {
  return composeIssueStateReviewerOperationsCallbacks<
    BotContext<RequestEvents>,
    IssueParams,
    IssueLike,
    TemplateLike,
    FormData,
    EffectiveConstants
  >({
    toLabelNames,
    normalizeKey,
    resolveWorkflowLabel,
    labelsMatching,
    resolveEffectiveConstants,
    extractResourceNameFromForm,
    resolveEffectiveRequestType,
    runApprovalHook,
    getHttpStatus,
    getErrorMessage,
    log,
  });
}

function buildApprovalCommentHandlingCallbacks(): ApprovalCommentHandlingCallbacks<
  BotContext<RequestEvents>,
  IssueParams,
  IssueLike,
  TemplateLike,
  FormData,
  EffectiveConstants,
  ValidateRequestIssueResult
> {
  return composeApprovalCommentHandlingCallbacks<
    BotContext<RequestEvents>,
    IssueParams,
    IssueLike,
    TemplateLike,
    FormData,
    EffectiveConstants,
    ValidateRequestIssueResult
  >({
    resolveEffectiveConstants,
    resolveEffectiveRequestType,
    resolveApproversForRequestType,
    ensureReviewLabelsPresentOnIssue,
    postOnce,
    uniqLogins,
    isAuthorizedApprover,
    resolveAdditionalIssueApproversFromApprovalHook,
    validateRequestIssue,
    setStateLabel,
    checkParentChainExistsInFlatStructure,
    log,
    finalizeApprovedRequest,
  });
}

function buildOwnerApprovalCommentHandlingCallbacks(): OwnerApprovalCommentHandlingCallbacks<
  BotContext<RequestEvents>,
  IssueParams,
  IssueLike,
  TemplateLike,
  FormData,
  ValidateRequestIssueResult,
  ContactApprovalMeta,
  ParentApprovalMeta
> {
  return composeOwnerApprovalCommentHandlingCallbacks<
    BotContext<RequestEvents>,
    IssueParams,
    IssueLike,
    TemplateLike,
    FormData,
    ValidateRequestIssueResult,
    ContactApprovalMeta,
    ParentApprovalMeta
  >({
    readContactApprovalMeta,
    readParentApprovalMeta,
    normalizeLogin,
    uniqLogins,
    normalizeKey,
    postOnce,
    validateRequestIssue,
    setStateLabel,
    parseForm,
    calcSnapshotHash,
    resolveEffectiveRequestType,
    ensureContactApprovalMarker,
    ensureParentApprovalMarker,
    maybeHandleApprovalDecision: async (
      context,
      params,
      issue,
      template,
      parsedFormData,
      requestType,
      namespace,
      options
    ): Promise<ApprovalHandlingResult> =>
      await maybeHandleApprovalDecision(
        context,
        params,
        issue,
        template,
        parsedFormData,
        requestType,
        namespace,
        options as ReturnType<typeof buildApprovalDecisionDispatchOptions>
      ),
    buildApprovalDecisionDispatchOptions,
    resolveManualReviewApproverOverrideFromApprovalHook,
    resolveAdditionalIssueApproversFromApprovalHook,
    handoverToCpa: async (context, params, issue, nsType, namespace, labels, options): Promise<void> =>
      await handoverToCpa(
        context,
        params,
        issue,
        nsType,
        namespace,
        labels,
        options as ReturnType<typeof buildReviewHandoverOptions>
      ),
    buildReviewHandoverOptions: (): Record<string, unknown> =>
      buildReviewHandoverOptions() as unknown as Record<string, unknown>,
    setParentOwnerActionState,
    assignParentOwnersForApproval,
    clearParentOwnerActionState,
    isSubContextRequestType,
    finalizeApprovedRequest,
    toStringTrim,
  });
}

async function handleAuthorUpdateComment(
  app: Probot,
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData
): Promise<void> {
  await processAuthorUpdateCommentApplication(
    context,
    params,
    issue,
    template,
    parsedFormData,
    buildRequestIssueAuthorUpdateCallbacks(app)
  );
}

function resolveVendorRegistryRootForRequestHandler(context: BotContext<RequestEvents>): string {
  const cfg: NormalizedStaticConfig = context.resourceBotConfig ?? DEFAULT_CONFIG;
  const reqs = isPlainObject(cfg.requests) ? cfg.requests : {};
  const vendorEntry = isPlainObject(reqs['vendor']) ? reqs['vendor'] : null;
  const vendorRoot = normalizeRepoPath(vendorEntry ? vendorEntry['folderName'] : '').replace(/\/+$/, '');
  return vendorRoot || 'data/vendors';
}

async function checkParentChainExistsInFlatStructure(
  context: BotContext<RequestEvents>,
  { owner, repo }: RepoInfo,
  template: TemplateLike,
  formData: FormData,
  explicitResourceName?: string
): Promise<string | null> {
  return await checkParentChainExistsInFlatStructureApplication(
    context,
    { owner, repo },
    template,
    formData,
    {
      extractResourceNameFromForm,
      resolveVendorRegistryRoot: resolveVendorRegistryRootForRequestHandler,
      getHttpStatus,
    },
    explicitResourceName
  );
}

async function loadTemplateWithLabelRefresh(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike
): Promise<TemplateLike> {
  let labels = toLabelNames(issue?.labels);

  try {
    return await loadTemplate(context, {
      owner: params.owner,
      repo: params.repo,
      issueTitle: toStringTrim(issue?.title || ''),
      issueLabels: labels,
    });
  } catch (e: unknown) {
    const msg = e instanceof Error ? toStringTrim(e.message) : toStringTrim(e);
    if (!msg.includes('no routing label found')) throw e;

    labels = await fetchIssueLabels(context, params);

    return await loadTemplate(context, {
      owner: params.owner,
      repo: params.repo,
      issueTitle: toStringTrim(issue?.title || ''),
      issueLabels: labels,
    });
  }
}

async function tryLoadTemplateForLabels(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  labels: string[]
): Promise<TemplateLike | null> {
  try {
    return await loadTemplate(context, {
      owner: params.owner,
      repo: params.repo,
      issueTitle: toStringTrim(issue?.title || ''),
      issueLabels: labels,
    });
  } catch {
    return null;
  }
}

function isSubContextRequestType(requestType: unknown): boolean {
  const rt = normalizeTypeToken(requestType);
  return rt === 'subcontextnamespace' || rt === 'subcontext';
}

function getApprovedParentOwnerLogin(issueBody: unknown, target: string): string {
  const meta = readParentApprovalMeta(issueBody);
  if (!meta) return '';

  const approvedBy = normalizeLogin(meta.approvedBy);
  if (!approvedBy) return '';

  return normalizeKey(meta.target) === normalizeKey(target) ? approvedBy : '';
}

async function ensureContactApprovalMarker(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  meta: ContactApprovalMeta | null
): Promise<boolean> {
  return await ensureContactApprovalMarkerApplication(
    context,
    params,
    issue,
    meta,
    buildOwnerApprovalRequirementsCallbacks()
  );
}

async function ensureParentApprovalMarker(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  meta: ParentApprovalMeta | null
): Promise<boolean> {
  return await ensureParentApprovalMarkerApplication(
    context,
    params,
    issue,
    meta,
    buildOwnerApprovalRequirementsCallbacks()
  );
}

function buildOwnerApprovalRequirementsCallbacks(): OwnerApprovalRequirementsCallbacks<
  BotContext<RequestEvents>,
  IssueParams,
  IssueLike,
  TemplateLike,
  FormData,
  EffectiveConstants
> {
  return {
    normalizeKey,
    labelsMatching,
    updateIssueBody: async (context: BotContext<RequestEvents>, params: IssueParams, body: string): Promise<void> => {
      await createGitHubIssueUpdateGateway(context).updateIssue({ ...params, body });
    },
    readYamlFromRepo,
    extractParentContactCandidates,
    lookupGithubLoginsByEmail,
    resolveEffectiveConstants,
    resolveWorkflowLabel,
    fetchIssueLabels,
    removeExactLabelsFromIssue,
    ensureLabelsPresentOnce,
    ensureAssigneesPresent,
    postOnce,
    isSubContextRequestType,
    setStateLabel,
    log,
  };
}

function buildCompatibleRequestSnapshotHashes(
  issueBody: unknown,
  parsedFormData: FormData,
  template: TemplateLike
): string[] {
  const processedBody = readIssueBodyForProcessing(issueBody);
  const rawBody = String(issueBody || '');

  return Array.from(
    new Set(
      [calcSnapshotHash(parsedFormData, template, processedBody), calcSnapshotHash(parsedFormData, template, rawBody)]
        .map((value) => toStringTrim(value))
        .filter(Boolean)
    )
  );
}

async function detectSingleRoutingLabel(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  labels: string[]
): Promise<string> {
  return await detectSingleRoutingLabelApplication(context, params, issue, labels, buildIssueWorkflowGuardCallbacks());
}

async function ensureRoutingLockMarker(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  expectedLabel: string
): Promise<boolean> {
  return await ensureRoutingLockMarkerApplication(
    context,
    params,
    issue,
    expectedLabel,
    buildIssueWorkflowGuardCallbacks()
  );
}

async function enforceRoutingLabelLock(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  expectedLabel: string,
  opts?: { changedLabel?: string }
): Promise<boolean> {
  return await enforceRoutingLabelLockApplication(
    context,
    params,
    issue,
    expectedLabel,
    buildIssueWorkflowGuardCallbacks(),
    opts
  );
}

function buildIssueWorkflowGuardCallbacks(): IssueWorkflowGuardCallbacks<
  BotContext<RequestEvents>,
  IssueParams,
  IssueLike,
  TemplateLike,
  FormData,
  EffectiveConstants
> {
  return composeIssueWorkflowGuardCallbacks<
    BotContext<RequestEvents>,
    IssueParams,
    IssueLike,
    TemplateLike,
    FormData,
    EffectiveConstants
  >({
    tryLoadTemplateForLabels,
    normalizeKey,
    postOnce,
    fetchIssueLabels,
    toLabelNames,
    removeExactLabelsFromIssue,
    labelsMatching,
    loadTemplateWithLabelRefresh,
    parseForm,
    readIssueBodyForProcessing,
    isRequestIssue,
    resolveEffectiveConstants,
    resolveLockedWorkflowLabelKeys,
    resolveWorkflowLabel,
    resolveEffectiveRequestType,
    resolveApproverRoutingForRequestType,
    uniqLogins,
    isConfiguredApprover,
    setStateLabel,
    removeRejectedStatusLabel,
    removeProgressStatusLabels,
    log,
    getErrorMessage,
  });
}

async function handleClosedIssueWorkflowGuard(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike
): Promise<void> {
  await handleClosedIssueWorkflowGuardApplication(context, params, issue, buildIssueWorkflowGuardCallbacks());
}

async function handleIssueLabelChangeWorkflowGuard(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  action: string,
  changedLabel: string,
  senderLogin: string | undefined | null
): Promise<void> {
  await handleIssueLabelChangeWorkflowGuardApplication(
    context,
    params,
    issue,
    action,
    changedLabel,
    senderLogin,
    buildIssueWorkflowGuardCallbacks()
  );
}

async function normalizeIssueTitle(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData
): Promise<void> {
  try {
    const resourceName = extractResourceNameFromForm(parsedFormData, template);
    const rawPrefix = toStringTrim(template?.title || template?.name || 'Request');
    const prefix = head(rawPrefix);

    if (!prefix || !resourceName) return;

    const desiredTitle = `${prefix}: ${resourceName}`;
    if (toStringTrim(issue.title) === desiredTitle) return;

    await createGitHubIssueUpdateGateway(context).updateIssue({
      owner: params.owner,
      repo: params.repo,
      issue_number: params.issue_number,
      title: desiredTitle,
    });

    issue.title = desiredTitle;
  } catch (err: unknown) {
    log(context, 'warn', { err: err instanceof Error ? err.message : String(err) }, 'Failed to normalize issue title');
  }
}

async function closeOutdatedRequestPrs(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  template: TemplateLike,
  options: { parsedFormData?: FormData; currentHash?: string; acceptedHashes?: string[] } = {}
): Promise<void> {
  await closeOutdatedRequestPrsApplication(
    context,
    params,
    template,
    options,
    buildOutdatedRequestPrCleanupCallbacks()
  );
}

function buildOutdatedRequestPrCleanupCallbacks(): OutdatedRequestPrCleanupCallbacks<
  BotContext<RequestEvents>,
  PullRequestLike,
  TemplateLike,
  FormData,
  EffectiveConstants
> {
  return {
    parseForm,
    readIssueBodyForProcessing,
    buildCompatibleRequestSnapshotHashes,
    calcSnapshotHash,
    extractHashFromPrBody,
    findOpenIssuePrs,
    resolveEffectiveConstants,
    postOnce,
  };
}

export default function requestHandler(app: Probot): void {
  const getStaticConfig = createStaticConfigContextLoader<BotContext<RequestEvents>>(app.log || console);

  const buildDefaultBranchCheckSuiteReevaluationCallbacks = (): DefaultBranchCheckSuiteReevaluationCallbacks<
    BotContext<RequestEvents>,
    RepoInfo,
    SequentialRegistryPrResult
  > =>
    composeDefaultBranchCheckSuiteReevaluationCallbacks<
      BotContext<RequestEvents>,
      RepoInfo,
      SequentialRegistryPrResult
    >({
      readDefaultBranchFromPayload,
      getErrorMessage,
      getHttpStatus,
      getStaticConfig: async (context: BotContext<RequestEvents>, options: { forceReload: true }): Promise<unknown> =>
        await getStaticConfig(context, options),
      reevaluateOpenDirectPullRequestsAfterDefaultBranchPush,
      updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry,
      log,
    });

  const autoMergeRuntime = createAutoMergeRuntime({
    getStaticConfig,
    evaluateHeadGreenForApprovalReevaluation,
    listOpenPullRequests,
    readFreshPullRequest,
    isSequentialDirectRegistryPr,
    getSequentialRegistryPrActive,
    clearSequentialRegistryPrActive,
    markSequentialRegistryPrHeadSkipped,
    markSequentialRegistryPrActive,
    requestPullRequestBranchUpdate,
    isSequentialRegistryPrActiveBlocking,
    parseLinkedIssueNumberFromPr,
    isSnapshotManagedRequestPr,
    pullRequestTargetsBranch,
    isSequentialRegistryPrHeadSkipped,
    listChangedYamlFilesForPrWithFallback,
    shouldUpdatePullRequestBranch,
    isPullRequestApprovedForBranchMaintenance,
    waitForPullRequestMergeability,
    hasAutoApprovedPrHead,
    isCrossRepositoryPullRequest,
    tryMergeIfGreen,
    maybeHandleStandaloneDirectPrApproval,
    buildIssueParams: (repoInfo: RepoInfo, issueNumber: number) => ({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      issue_number: issueNumber,
    }),
    readLinkedIssue: async (context: BotContext<RequestEvents>, params: IssueParams): Promise<IssueLike> => {
      const res = await context.octokit.issues.get(params);
      return res.data as unknown as IssueLike;
    },
    log,
    getErrorMessage,
    getHttpStatus,
    hasIssueFormInputs,
    loadTemplateWithLabelRefresh,
    parseForm,
    readIssueBodyForProcessing,
    isRequestIssue,
    buildCompatibleRequestSnapshotHashes,
    calcSnapshotHash,
    extractHashFromPrBody,
    closeOutdatedRequestPrs,
    maybeHandleDirectPrApprovalForMerge,
    isPullRequestOpen,
  });

  const runOneSequentialDirectRegistryPrMaintenance = autoMergeRuntime.runOneSequentialDirectRegistryPrMaintenance;
  const tryMergeApprovedPrOrUpdateBranch = autoMergeRuntime.tryMergeApprovedPrOrUpdateBranch;
  const tryAutoMerge = autoMergeRuntime.tryAutoMerge;
  const handleBlockingRegistryHeadConclusion = autoMergeRuntime.handleBlockingRegistryHeadConclusion;

  function buildDefaultBranchDirectPrReevaluationCallbacks(): DefaultBranchDirectPrReevaluationCallbacks<
    BotContext<RequestEvents>,
    RepoInfo,
    SequentialRegistryPrResult
  > {
    return composeDefaultBranchDirectPrReevaluationCallbacks<
      BotContext<RequestEvents>,
      RepoInfo,
      SequentialRegistryPrResult
    >({
      runOneSequentialDirectRegistryPrMaintenance,
      log,
    });
  }

  async function reevaluateOpenDirectPullRequestsAfterDefaultBranchPush(
    context: BotContext<RequestEvents>,
    repoInfo: RepoInfo,
    baseBranch: string,
    reason = 'default-branch-push:direct-pr-reevaluation'
  ): Promise<SequentialRegistryPrResult> {
    return await reevaluateOpenDirectPullRequestsAfterDefaultBranchPushApplication(
      context,
      repoInfo,
      baseBranch,
      buildDefaultBranchDirectPrReevaluationCallbacks(),
      reason
    );
  }

  function buildDirectPrApprovalCommentHandlingCallbacks(): DirectPrApprovalCommentHandlingCallbacks<
    BotContext<RequestEvents>,
    RepoInfo,
    IssueParams,
    PullRequestLike,
    IssueLike,
    EffectiveConstants
  > {
    return composeDirectPrApprovalCommentHandlingCallbacks<
      BotContext<RequestEvents>,
      RepoInfo,
      IssueParams,
      PullRequestLike,
      IssueLike,
      EffectiveConstants
    >({
      resolveEffectiveConstants,
      prAsIssueLike,
      ensureReviewLabelsPresentOnIssue,
      resolveDirectPrRequestTypes,
      resolveAllowedApproversForRequestTypes,
      evaluateDirectPrOnApproval,
      postApprovalRejectedOnce,
      isAuthorizedApprover,
      ensureAutomatedApprovalReviewForCurrentHead,
      isCrossRepositoryPullRequest,
      tryMergeApprovedPrOrUpdateBranch,
      postOnce,
      log,
    });
  }

  async function handleDirectPrApprovalComment(
    context: BotContext<RequestEvents>,
    repoInfo: RepoInfo,
    pr: PullRequestLike,
    commenter: string
  ): Promise<void> {
    await handleDirectPrApprovalCommentApplication(
      context,
      repoInfo,
      pr,
      commenter,
      buildDirectPrApprovalCommentHandlingCallbacks()
    );
  }

  const shouldSkipIssueEditedEvent = (
    context: BotContext<'issues.opened' | 'issues.edited' | 'issues.reopened'>
  ): boolean => {
    const payload = context.payload as unknown;

    const action = isPlainObject(payload) ? toStringTrim(payload['action']) : '';
    if (action !== 'edited') return false;

    const changes = isPlainObject(payload) && 'changes' in payload ? payload['changes'] : undefined;
    const chObj = isPlainObject(changes) ? changes : {};

    const bodyOrLabelChanged = Boolean(chObj['body']) || Boolean(chObj['labels']);
    return !bodyOrLabelChanged;
  };

  // normalizeIssueTitle moved to outer scope
  const isApprovalCommentForContext = (context: BotContext<RequestEvents>, strippedText: string): boolean => {
    const cfg: NormalizedStaticConfig = context.resourceBotConfig ?? DEFAULT_CONFIG;
    const wf = cfg?.workflow ?? {};
    let labelsCfg: Record<string, unknown> = {};
    if (isPlainObject(wf)) {
      const raw = (wf as Record<string, unknown>)['labels'];
      if (isPlainObject(raw)) labelsCfg = raw;
    }

    const approvalSuccessful = labelsCfg['approvalSuccessful'];
    let approvalKeyword = '';
    if (Array.isArray(approvalSuccessful)) approvalKeyword = toStringTrim(approvalSuccessful[0]);
    else if (approvalSuccessful !== undefined && approvalSuccessful !== null) {
      approvalKeyword = toStringTrim(approvalSuccessful);
    }

    return isApprovalComment(strippedText, approvalKeyword);
  };

  // moved to outer scope
  const handleIssueLifecycle = createIssueLifecycleEventHandler<
    BotContext<'issues.opened' | 'issues.edited' | 'issues.reopened'>,
    IssueParams,
    IssueLike,
    SenderLike
  >({
    getStaticConfig: async (
      context: BotContext<'issues.opened' | 'issues.edited' | 'issues.reopened'>
    ): Promise<unknown> => await getStaticConfig(context),
    shouldSkipIssueEditedEvent,
    isPlainObject,
    toStringTrim,
    isBotSender,
    toLabelNames,
    processIssueEvent: async (
      context: BotContext<'issues.opened' | 'issues.edited' | 'issues.reopened'>,
      params: IssueParams,
      issue: IssueLike
    ): Promise<void> => await processIssueEvent(app, context, params, issue),
    log,
    isDebugEnabled: DBG,
  });

  const handleIssueClosed = createIssueClosedEventHandler<BotContext<'issues.closed'>, IssueParams, IssueLike>({
    getStaticConfig: async (context: BotContext<'issues.closed'>): Promise<unknown> => await getStaticConfig(context),
    hasIssueFormInputs,
    isJestWorker: (): boolean => Boolean(process.env.JEST_WORKER_ID),
    handleClosedIssueWorkflowGuard: async (
      context: BotContext<'issues.closed'>,
      params: IssueParams,
      issue: IssueLike
    ): Promise<void> => await handleClosedIssueWorkflowGuard(context, params, issue),
  });

  const handleIssueLabelChange = createIssueLabelChangeEventHandler<
    BotContext<'issues.labeled' | 'issues.unlabeled'>,
    IssueParams,
    IssueLike,
    SenderLike
  >({
    getStaticConfig: async (context: BotContext<'issues.labeled' | 'issues.unlabeled'>): Promise<unknown> =>
      await getStaticConfig(context),
    isBotSender,
    hasIssueFormInputs,
    isJestWorker: (): boolean => Boolean(process.env.JEST_WORKER_ID),
    toStringTrim,
    readPayloadLabelName,
    handleIssueLabelChangeWorkflowGuard: async (
      context: BotContext<'issues.labeled' | 'issues.unlabeled'>,
      params: IssueParams,
      issue: IssueLike,
      action: string,
      changedLabel: string,
      senderLogin: string | undefined | null
    ): Promise<void> =>
      await handleIssueLabelChangeWorkflowGuard(context, params, issue, action, changedLabel, senderLogin),
  });

  const handleIssueComment = createIssueCommentEventHandler<
    BotContext<'issue_comment.created' | 'issue_comment.edited'>,
    IssueParams,
    IssueLike,
    CommentLike,
    SenderLike,
    PullRequestLike,
    TemplateLike,
    FormData
  >({
    getStaticConfig: async (context: BotContext<'issue_comment.created' | 'issue_comment.edited'>): Promise<unknown> =>
      await getStaticConfig(context),
    isPlainObject,
    isBotSender,
    hasIssueFormInputs,
    isJestWorker: (): boolean => Boolean(process.env.JEST_WORKER_ID),
    stripQuoteAndCode,
    isApprovalCommentForContext: (
      context: BotContext<'issue_comment.created' | 'issue_comment.edited'>,
      strippedText: string
    ): boolean => isApprovalCommentForContext(context, strippedText),
    isAuthorUpdateComment,
    readFreshPullRequest: async (
      context: BotContext<'issue_comment.created' | 'issue_comment.edited'>,
      repoInfo: RepoInfo,
      prNumber: number
    ): Promise<PullRequestLike | null> => await readFreshPullRequest(context, repoInfo, prNumber),
    parseLinkedIssueNumberFromPr,
    handleDirectPrApprovalComment: async (
      context: BotContext<'issue_comment.created' | 'issue_comment.edited'>,
      repoInfo: RepoInfo,
      pr: PullRequestLike,
      commenter: string
    ): Promise<void> => await handleDirectPrApprovalComment(context, repoInfo, pr, commenter),
    loadTemplateWithLabelRefresh: async (
      context: BotContext<'issue_comment.created' | 'issue_comment.edited'>,
      params: IssueParams,
      issue: IssueLike
    ): Promise<TemplateLike> => await loadTemplateWithLabelRefresh(context, params, issue),
    readIssueBodyForProcessing,
    parseForm,
    isRequestIssue: (
      context: BotContext<'issue_comment.created' | 'issue_comment.edited'>,
      template: TemplateLike,
      parsedFormData: FormData
    ): boolean => isRequestIssue(context, template, parsedFormData),
    handleParentOwnerApprovalIfNeeded: async (
      context: BotContext<'issue_comment.created' | 'issue_comment.edited'>,
      params: IssueParams,
      issue: IssueLike,
      template: TemplateLike,
      parsedFormData: FormData,
      commenter: string
    ): Promise<boolean> =>
      await handleParentOwnerApprovalIfNeeded(context, params, issue, template, parsedFormData, commenter),
    handleSystemContactOwnerApprovalIfNeeded: async (
      context: BotContext<'issue_comment.created' | 'issue_comment.edited'>,
      params: IssueParams,
      issue: IssueLike,
      template: TemplateLike,
      parsedFormData: FormData,
      commenter: string
    ): Promise<boolean> =>
      await handleSystemContactOwnerApprovalIfNeeded(context, params, issue, template, parsedFormData, commenter),
    handleApprovalComment: async (
      context: BotContext<'issue_comment.created' | 'issue_comment.edited'>,
      params: IssueParams,
      issue: IssueLike,
      template: TemplateLike,
      parsedFormData: FormData,
      commenter: string
    ): Promise<void> => await handleApprovalComment(context, params, issue, template, parsedFormData, commenter),
    handleAuthorUpdateComment: async (
      context: BotContext<'issue_comment.created' | 'issue_comment.edited'>,
      params: IssueParams,
      issue: IssueLike,
      template: TemplateLike,
      parsedFormData: FormData
    ): Promise<void> => await handleAuthorUpdateComment(app, context, params, issue, template, parsedFormData),
    log,
    isDebugEnabled: DBG,
  });

  const maybeHandleDefaultBranchCheckSuiteSuccess = async (
    context: BotContext<RequestEvents>,
    payload: unknown,
    checkSuite: CheckSuiteLike | null,
    repoInfo: RepoInfo
  ): Promise<void> => {
    await maybeHandleDefaultBranchCheckSuiteSuccessApplication(
      context,
      payload,
      checkSuite,
      repoInfo,
      buildDefaultBranchCheckSuiteReevaluationCallbacks()
    );
  };

  const buildCheckCompletedHandlerCallbacks = (): CheckCompletedHandlerCallbacks<
    BotContext<RequestEvents>,
    RepoInfo,
    CheckRunLike,
    CheckSuiteLike,
    {
      byFile: Map<string, string[]>;
      machineReadableSources: RegistryValidationMachineReadableSource[];
    },
    RegistryValidationMachineReadableSource
  > =>
    composeCheckCompletedHandlerCallbacks<
      BotContext<RequestEvents>,
      RepoInfo,
      CheckRunLike,
      CheckSuiteLike,
      {
        byFile: Map<string, string[]>;
        machineReadableSources: RegistryValidationMachineReadableSource[];
      },
      RegistryValidationMachineReadableSource
    >({
      readCheckRunFromPayload,
      readCheckSuiteFromPayload,
      readRepoInfoFromPayload,
      readCheckRunPrNumbers,
      resolveCheckSuitePrNumbers,
      readCheckSuiteId,
      listAllCheckRunsForSuite,
      readCheckRunId,
      readFirstRegistryValidationArtifactsForSuiteRuns,
      collapseBotCommentsByPrefix,
      postCheckSuiteRegistryValidationComments,
      maybeHandleDefaultBranchCheckSuiteSuccess,
      tryAutoMerge,
      maybeApprovePendingWorkflowRunsForPrNumbers,
      handleBlockingRegistryHeadConclusion,
      isBlockingCheckConclusion,
      readDefaultBranchFromPayload,
      getStaticConfig: async (context: BotContext<RequestEvents>): Promise<unknown> => await getStaticConfig(context),
      log,
      isDebugEnabled: DBG,
      toStringTrim,
    });

  const handlePush = createPushEventHandler<BotContext<'push'>, RepoInfo, SequentialRegistryPrResult>({
    readRepoInfoFromPayload,
    isPlainObject,
    toStringTrim,
    readDefaultBranchFromPush,
    readPushChangedFiles,
    isApprovalConfigChangePath,
    isDefaultBranchPush,
    getStaticConfig: async (context: BotContext<'push'>, options: { forceReload?: boolean }): Promise<unknown> =>
      await getStaticConfig(context, options),
    reevaluateOpenDirectPullRequestsAfterDefaultBranchPush: async (
      context: BotContext<'push'>,
      repoInfo: RepoInfo,
      baseBranch: string,
      reason: string
    ): Promise<SequentialRegistryPrResult> =>
      await reevaluateOpenDirectPullRequestsAfterDefaultBranchPush(context, repoInfo, baseBranch, reason),
    updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry: async (
      context: BotContext<'push'>,
      repoInfo: RepoInfo,
      baseBranch: string
    ): Promise<boolean> =>
      await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry(context, repoInfo, baseBranch),
    log,
  });

  const handlePullRequest = createPullRequestEventHandler<
    BotContext<
      'pull_request.opened' | 'pull_request.synchronize' | 'pull_request.reopened' | 'pull_request.ready_for_review'
    >,
    RepoInfo,
    PullRequestLike
  >({
    getStaticConfig,
    readRepoInfoFromPayload,
    isPlainObject,
    isPullRequestOpen,
    maybeApprovePendingWorkflowRunsForRegistryPrWithRetry,
    toStringTrim,
  });

  const handleCheck = createCheckEventHandler<BotContext<'check_suite.completed' | 'check_run.completed'>>({
    toStringTrim,
    handleCheckCompletedEvent: async (context, payload, eventName): Promise<void> => {
      await handleCheckCompletedEventApplication(context, payload, eventName, buildCheckCompletedHandlerCallbacks());
    },
  });

  const handleStatus = createStatusEventHandler<BotContext<'status'>, RepoInfo>({
    isPlainObject,
    toStringTrim,
    toRepoInfo: (owner, repo) => ({ owner, repo }),
    tryAutoMerge,
  });

  registerRequestEvents(app, {
    handlePush,
    handlePullRequest,
    handleCheck,
    handleStatus,
    handleIssueLifecycle,
    handleIssueClosed,
    handleIssueLabelChange,
    handleIssueComment,
  });
}
