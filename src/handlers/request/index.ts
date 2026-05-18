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
import {
  buildMachineReadableMetadataBlock,
  normalizeMachineReadableIssues,
  type MachineReadableIssue,
} from './domain/machine-readable.js';
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
  requestPullRequestBranchUpdateRespectingSequentialRegistryQueue as requestPullRequestBranchUpdateRespectingSequentialRegistryQueueApplication,
  type BranchUpdateSequentialHandoffCallbacks,
} from './application/branch-update-sequential-handoff.js';
import {
  runOneSequentialDirectRegistryPrMaintenance as runOneSequentialDirectRegistryPrMaintenanceApplication,
  type SequentialRegistryPrQueueCallbacks,
} from './application/sequential-registry-pr-queue.js';
import {
  advanceSequentialRegistryPrQueueAfterTerminalState as advanceSequentialRegistryPrQueueAfterTerminalStateApplication,
  handleBlockingRegistryHeadConclusion as handleBlockingRegistryHeadConclusionApplication,
  releaseSequentialRegistryPrIfNotApprovedAfterGreen as releaseSequentialRegistryPrIfNotApprovedAfterGreenApplication,
  type SequentialRegistryPrTerminalCallbacks,
} from './application/sequential-registry-pr-terminal.js';
import {
  runAutoMergeEvaluation as runAutoMergeEvaluationApplication,
  tryAutoMerge as tryAutoMergeApplication,
  type AutoMergeTriggerCallbacks,
} from './application/auto-merge-trigger.js';
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
  clearSequentialRegistryPrActive,
  getSequentialRegistryPrActive,
  isSequentialRegistryPrHeadSkipped,
  markSequentialRegistryPrActive as markSequentialRegistryPrActiveState,
  markSequentialRegistryPrHeadSkipped as markSequentialRegistryPrHeadSkippedState,
  type SequentialRegistryPrActive as SequentialRegistryPrActiveState,
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
import { tryMergeApprovedPrOrUpdateBranch as tryMergeApprovedPrOrUpdateBranchApplication } from './application/merge-inflight.js';
import {
  runMergeApprovedPrOrUpdateBranch as runMergeApprovedPrOrUpdateBranchApplication,
  type MergeApprovedPrOrUpdateBranchCallbacks,
} from './application/merge-approved-pr-or-update-branch.js';
import { handoverStandaloneDirectPrToReview } from './application/pr-review-handover.js';
import { handoverToCpa } from './application/review-handover.js';
import { maybeHandleApprovalDecision } from './application/approval-decision-dispatch.js';
import { rejectRequestFromApprovalHook } from './application/approval-rejection.js';
import { postApprovalRejectedOnce, postApprovalUnknownOnce } from './application/approval-outcome-posting.js';
import {
  handleApprovalComment as handleApprovalCommentApplication,
  type ApprovalCommentHandlingCallbacks,
} from './application/approval-comment-handling.js';
import {
  handleParentOwnerApprovalIfNeeded as handleParentOwnerApprovalIfNeededApplication,
  handleSystemContactOwnerApprovalIfNeeded as handleSystemContactOwnerApprovalIfNeededApplication,
  type OwnerApprovalCommentHandlingCallbacks,
} from './application/owner-approval-comment-handling.js';
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
  finalizeApprovedRequest as finalizeApprovedRequestApplication,
  type ApprovedRequestFinalizationCallbacks,
} from './application/approved-request-finalization.js';
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
import { normalizeApprovalDecision, type ApprovalDecision } from './domain/approval-decision.js';
import { isAuthorizedApprover as isAuthorizedApproverPure } from './domain/approval-authorization.js';
import {
  normalizeLogin as normalizeLoginPure,
  toStringTrim as toStringTrimPure,
  uniqLogins as uniqLoginsPure,
} from './domain/login-utils.js';
import { buildAutoApprovalReviewMarker as buildAutoApprovalReviewMarkerPure } from './domain/auto-approval-review-marker.js';
import { getUnknownManualApprovers, getVisibleApprovalText } from './domain/approval-policy.js';
import { buildRoutingLockBody, readRoutingLockExpected } from './domain/routing-lock-marker.js';
import {
  buildContactApprovalBody,
  buildParentApprovalBody,
  readContactApprovalMeta,
  readParentApprovalMeta,
  type ContactApprovalMeta,
  type ParentApprovalMeta,
} from './domain/approval-markers.js';
import { readIssueBodyForProcessing } from './domain/issue-body-processing.js';
import {
  buildRegistryValidationAggregateBody,
  buildRegistryValidationCommentHeading,
  extractFieldFromMsg,
  filterMachineReadableSourcesForFile,
  filterRegistryValidationEntries,
  normalizeMsg,
  type RegistryValidationMachineReadableSource,
} from './domain/registry-validation-annotations.js';
import { isApprovalComment, isAuthorUpdateComment, stripQuoteAndCode } from './domain/comment-commands.js';
import { tryMergeIfGreen as tryMergeIfGreenRaw } from '../../lib/auto-merge.js';
import { loadStaticConfig, DEFAULT_CONFIG, type NormalizedStaticConfig, type RegistryBotHooks } from '../../config.js';
import { getDocLinksFromConfig } from './constants.js';
import type { Context, Probot } from 'probot';
import YAML from 'yaml';
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
  | 'check_suite.completed'
  | 'check_run.completed'
  | 'status'
  | 'push';

type ResourceBotContextExt = {
  resourceBotConfig?: NormalizedStaticConfig;
  resourceBotHooks?: RegistryBotHooks | null;
  resourceBotHooksSource?: string | null;
};

type StaticConfigLoadOptions = {
  forceReload?: boolean;
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

type GitTreeLike = {
  tree?: GitTreeEntryLike[];
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

type SchemaFieldAliasLookup = Map<string, string>;

type LogLevel = 'debug' | 'info' | 'warn' | 'error';
type LoggerFn = (this: unknown, obj: unknown, msg?: string) => void;
type LoggerLike = Partial<Record<LogLevel, LoggerFn>>;

const log = (context: { log?: LoggerLike } | undefined, level: LogLevel, obj: unknown, msg: string): void => {
  const logger = context?.log;
  const fn = logger?.[level];

  if (typeof fn === 'function') {
    fn.call(logger, obj, msg);
  }
};

const SCHEMA_FIELD_ALIAS_CACHE = new Map<string, Promise<SchemaFieldAliasLookup>>();

function normalizeSchemaFieldAlias(value: unknown): string {
  const raw = toStringTrim(value);
  if (!raw) return '';

  return raw
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
}

function addSchemaFieldAlias(lookup: SchemaFieldAliasLookup, aliasValue: unknown, propertyName: string): void {
  const alias = normalizeSchemaFieldAlias(aliasValue);
  if (!alias || lookup.has(alias)) return;

  lookup.set(alias, propertyName);

  if (alias.endsWith('s') && alias.length > 1) {
    const singular = alias.slice(0, -1);
    if (singular && !lookup.has(singular)) lookup.set(singular, propertyName);
  } else {
    const plural = `${alias}s`;
    if (!lookup.has(plural)) lookup.set(plural, propertyName);
  }
}

function collectSchemaFieldAliasesForProperty(
  propertyName: string,
  propertyDef: unknown,
  lookup: SchemaFieldAliasLookup
): void {
  addSchemaFieldAlias(lookup, propertyName, propertyName);
  if (!isPlainObject(propertyDef)) return;

  addSchemaFieldAlias(lookup, propertyDef['title'], propertyName);
  addSchemaFieldAlias(lookup, propertyDef['x-form-field'], propertyName);
  collectSchemaFieldAliases(propertyDef, lookup);
}

function collectSchemaFieldAliasesFromProperties(props: Record<string, unknown>, lookup: SchemaFieldAliasLookup): void {
  for (const [propertyName, propertyDef] of Object.entries(props)) {
    collectSchemaFieldAliasesForProperty(propertyName, propertyDef, lookup);
  }
}

function collectSchemaFieldAliasesFromArray(items: unknown[], lookup: SchemaFieldAliasLookup): void {
  for (const item of items) {
    collectSchemaFieldAliases(item, lookup);
  }
}

function collectSchemaFieldAliases(schemaObj: unknown, lookup: SchemaFieldAliasLookup): void {
  if (!isPlainObject(schemaObj)) return;

  const props = isPlainObject(schemaObj['properties']) ? schemaObj['properties'] : null;
  if (props) collectSchemaFieldAliasesFromProperties(props, lookup);

  for (const key of ['allOf', 'anyOf', 'oneOf'] as const) {
    const items = schemaObj[key];
    if (!Array.isArray(items)) continue;

    collectSchemaFieldAliasesFromArray(items, lookup);
  }

  const defs = isPlainObject(schemaObj['$defs']) ? schemaObj['$defs'] : null;
  if (defs) {
    for (const value of Object.values(defs)) {
      collectSchemaFieldAliases(value, lookup);
    }
  }
}

async function loadSchemaFieldAliasLookup(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  schemaPath: string
): Promise<SchemaFieldAliasLookup> {
  const rawPath = toStringTrim(schemaPath);
  if (!rawPath) return new Map<string, string>();

  const cleaned = rawPath.replace(/^\.?\//, '');
  const candidates = rawPath.startsWith('/')
    ? [rawPath.replace(/^\/+/, '')]
    : [cleaned.startsWith('.github/') ? cleaned : `.github/registry-bot/${cleaned}`, cleaned];

  const cacheKey = `${repoInfo.owner}/${repoInfo.repo}:${JSON.stringify(candidates)}`;
  const cached = SCHEMA_FIELD_ALIAS_CACHE.get(cacheKey);
  if (cached) return await cached;

  const pending = (async (): Promise<SchemaFieldAliasLookup> => {
    for (const candidate of candidates) {
      const raw = await readRepoFileText(context, repoInfo, candidate);
      if (!raw) continue;

      try {
        const parsed = JSON.parse(raw) as unknown;
        const lookup = new Map<string, string>();
        collectSchemaFieldAliases(parsed, lookup);
        return lookup;
      } catch {
        continue;
      }
    }

    return new Map<string, string>();
  })();

  SCHEMA_FIELD_ALIAS_CACHE.set(cacheKey, pending);
  return await pending;
}

async function resolveMachineReadableRegistryField(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  fieldHint: string,
  schemaPath?: string
): Promise<string> {
  const fallback = toStringTrim(fieldHint) || 'details';
  const normalizedSchemaPath = toStringTrim(schemaPath);

  if (!normalizedSchemaPath || fallback === 'details') return fallback;

  const lookup = await loadSchemaFieldAliasLookup(context, repoInfo, normalizedSchemaPath);
  if (!lookup.size) return fallback;

  return lookup.get(normalizeSchemaFieldAlias(fallback)) || fallback;
}

async function buildRegistryValidationMachineReadableIssues(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  items: RegistryValidationMachineReadableSource[]
): Promise<MachineReadableIssue[]> {
  const out: MachineReadableIssue[] = [];

  for (const item of items || []) {
    const message = normalizeMsg(item.message);
    if (!message) continue;

    const fieldHint = extractFieldFromMsg(item.message) || 'details';
    const field = await resolveMachineReadableRegistryField(context, repoInfo, fieldHint, item.schemaPath);
    const normalizedFilePath = toStringTrim(item.filePath);

    out.push({
      field,
      message,
      ...(normalizedFilePath ? { filePath: normalizedFilePath } : {}),
    });
  }

  return normalizeMachineReadableIssues(out);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function getHttpStatus(err: unknown): number | undefined {
  if (!isPlainObject(err)) return undefined;
  const status = err['status'];
  return typeof status === 'number' ? status : undefined;
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

type RepoContentFile = { content?: string; encoding?: string };

function isRepoContentFile(v: unknown): v is RepoContentFile {
  return isPlainObject(v) && typeof v['content'] === 'string';
}

async function readRepoFileText(
  context: BotContext<RequestEvents>,
  repo: RepoInfo,
  path: string
): Promise<string | null> {
  const p = toStringTrim(path).replace(/^\/+/, '');
  if (!p) return null;

  try {
    const res = await context.octokit.repos.getContent({ owner: repo.owner, repo: repo.repo, path: p });
    const data = (res as unknown as { data?: unknown }).data;

    if (Array.isArray(data) || !isRepoContentFile(data)) return null;

    const enc = typeof data.encoding === 'string' ? data.encoding : 'base64';
    return Buffer.from(String(data.content || ''), enc as BufferEncoding).toString('utf8');
  } catch {
    return null;
  }
}

async function readYamlFromRepo(
  context: BotContext<RequestEvents>,
  repo: RepoInfo,
  path: string
): Promise<unknown | null> {
  const txt = await readRepoFileText(context, repo, path);
  if (!txt) return null;

  try {
    return YAML.parse(txt);
  } catch {
    return null;
  }
}

const LOGIN_RE = /^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$/;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function extractParentContactCandidates(value: unknown): { logins: string[]; emails: string[] } {
  const logins: string[] = [];
  const emails: string[] = [];

  const pushLogin = (v: unknown): void => {
    const s = normalizeLogin(v);
    if (!s) return;
    if (!LOGIN_RE.test(s)) return;
    logins.push(s);
  };

  const pushEmail = (v: unknown): void => {
    const s = toStringTrim(v);
    if (!s) return;
    const t = s.replace(/^<|>$/g, '').trim();
    if (!EMAIL_RE.test(t)) return;
    emails.push(t);
  };

  const fromString = (raw: string, strongLoginHint: boolean): void => {
    const s = toStringTrim(raw);
    if (!s) return;

    const urlM =
      /(?:https?:\/\/)?(?:www\.)?(?:github\.com|github\.tools\.sap)\/([A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?)/gi;
    for (const m of s.matchAll(urlM)) {
      if (m?.[1]) pushLogin(m[1]);
    }

    const tokens = s
      .split(/[,\s;]+/)
      .map((t) => t.trim())
      .filter(Boolean);

    for (let t of tokens) {
      t = t.replace(/^[<([{"']+|[>)\]},"']+$/g, '').trim();
      if (!t) continue;

      if (t.includes('@') && EMAIL_RE.test(t)) {
        pushEmail(t);
        continue;
      }

      if (t.startsWith('@')) {
        const u = t.slice(1);
        if (u && !u.includes('.') && LOGIN_RE.test(u)) pushLogin(u);
        continue;
      }

      if ((strongLoginHint || tokens.length === 1) && !t.includes('.') && LOGIN_RE.test(t)) {
        pushLogin(t);
      }
    }
  };

  const walk = (v: unknown, keyHint?: string): void => {
    if (v === null || v === undefined) return;

    if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') {
      const k = String(keyHint || '').toLowerCase();
      const strong = ['github', 'login', 'username', 'user', 'owner', 'id', 'uid', 'account', 'gh'].some((x) =>
        k.includes(x)
      );
      fromString(String(v), strong);
      return;
    }

    if (Array.isArray(v)) {
      for (const el of v) walk(el, keyHint);
      return;
    }

    if (isPlainObject(v)) {
      for (const [k, vv] of Object.entries(v)) walk(vv, k);
      return;
    }
  };

  walk(value);

  return { logins: uniqLogins(logins), emails: Array.from(new Set(emails.map((e) => e.toLowerCase()))) };
}

const EMAIL_TO_LOGINS_CACHE = new Map<string, Promise<string[]>>();

async function lookupGithubLoginsByEmail(context: BotContext<RequestEvents>, email: string): Promise<string[]> {
  const e = toStringTrim(email).toLowerCase();
  if (!e || !e.includes('@')) return [];

  const cached = EMAIL_TO_LOGINS_CACHE.get(e);
  if (cached) return await cached;

  const p = (async (): Promise<string[]> => {
    const found: string[] = [];
    const q = `${e} in:email`;

    try {
      const res = await context.octokit.search.users({ q, per_page: 5 });
      const items = (res as unknown as { data?: { items?: { login?: string }[] } })?.data?.items ?? [];
      for (const it of items) {
        const login = normalizeLogin(it?.login);
        if (login) found.push(login);
      }
    } catch {
      /* empty */
    }

    if (found.length) return uniqLogins(found);

    try {
      const gql = `
        query($q: String!) {
          search(type: USER, query: $q, first: 5) {
            nodes { ... on User { login } }
          }
        }
      `;
      const r = await (
        context.octokit as unknown as {
          graphql: (q: string, v: unknown) => Promise<{ search?: { nodes?: { login?: string }[] } }>;
        }
      ).graphql(gql, { q });

      const nodes = r?.search?.nodes ?? [];
      for (const n of nodes) {
        const login = normalizeLogin(n?.login);
        if (login) found.push(login);
      }
    } catch {
      /* empty */
    }

    return uniqLogins(found);
  })();

  EMAIL_TO_LOGINS_CACHE.set(e, p);
  return await p;
}

async function resolveParentOwnerLoginsForTarget(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  template: TemplateLike,
  validatedNamespace: string,
  requestType: string
): Promise<{ parent: string; owners: string[] }> {
  const rt = toStringTrim(requestType).toLowerCase();
  if (!rt.includes('namespace')) return { parent: '', owners: [] };

  const target = toStringTrim(validatedNamespace);
  const parts = target
    .split('.')
    .map((p) => p.trim())
    .filter(Boolean);

  if (parts.length <= 2) return { parent: '', owners: [] };

  const parent = parts.slice(0, -1).join('.');
  if (!parent) return { parent: '', owners: [] };

  const rootRaw = toStringTrim(template?._meta?.root);
  const root = rootRaw.replace(/^\/+/, '').replace(/\/+$/, '');
  if (!root) return { parent, owners: [] };

  const parentPath = `${root}/${parent}.yaml`;
  const doc = await readYamlFromRepo(context, { owner: params.owner, repo: params.repo }, parentPath);
  if (!isPlainObject(doc)) return { parent, owners: [] };

  const rec = doc;
  const contacts = rec['contacts'] ?? rec['contact'] ?? rec['owners'] ?? rec['owner'];

  const { logins: directLogins, emails } = extractParentContactCandidates(contacts);

  const resolved: string[] = [...directLogins];
  for (const email of emails.slice(0, 10)) {
    resolved.push(...(await lookupGithubLoginsByEmail(context, email)));
  }

  return { parent, owners: uniqLogins(resolved) };
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
    ): Promise<{ data?: unknown }> => await context.octokit.checks.listForSuite(args),
    listCheckRunAnnotations: async (
      context: BotContext<RequestEvents>,
      args: {
        owner: string;
        repo: string;
        check_run_id: number;
        per_page: number;
        page: number;
      }
    ): Promise<{ data?: unknown }> => await context.octokit.checks.listAnnotations(args),
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

async function buildRegistryValidationPrCommentBody(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  filePath: string,
  messages: string[],
  machineReadableSources: RegistryValidationMachineReadableSource[]
): Promise<string> {
  const lines: string[] = [
    '## Detected issues',
    '',
    ...buildRegistryValidationCommentHeading(filePath, messages, '###'),
  ];

  const body = lines.join('\n').trimEnd();
  const machineReadable = await buildRegistryValidationMachineReadableIssues(context, repoInfo, machineReadableSources);

  return `${body}

${buildMachineReadableMetadataBlock(machineReadable)}`;
}

async function buildRegistryValidationAggregatePrCommentBody(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  byFile: Map<string, string[]>,
  machineReadableSources: RegistryValidationMachineReadableSource[]
): Promise<string> {
  const entries = filterRegistryValidationEntries(byFile);

  if (!entries.length) return '';
  if (entries.length === 1) {
    const [filePath, messages] = entries[0];
    return await buildRegistryValidationPrCommentBody(
      context,
      repoInfo,
      filePath,
      messages,
      filterMachineReadableSourcesForFile(machineReadableSources, filePath)
    );
  }

  const body = buildRegistryValidationAggregateBody(byFile);

  const machineReadable = await buildRegistryValidationMachineReadableIssues(context, repoInfo, machineReadableSources);

  return `${body}

${buildMachineReadableMetadataBlock(machineReadable)}`;
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
    ): Promise<{ data?: PullRequestLike[] }> => await context.octokit.pulls.list(args),
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
  const { data } = await context.octokit.issues.get({ owner, repo, issue_number });
  const issue = data as unknown as IssueLike;
  return toLabelNames(issue.labels);
}

const ENSURE_LABELS_INFLIGHT = new Map<string, Promise<void>>();

function issueScopedKey(params: IssueParams, suffix: string): string {
  return `${params.owner}/${params.repo}#${params.issue_number}:${suffix}`.toLowerCase();
}

async function ensureLabelsPresentOnce(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  labels: string[]
): Promise<void> {
  const targetLabels = Array.from(new Set((labels || []).map(toStringTrim).filter(Boolean)));
  if (!targetLabels.length) return;

  const key = issueScopedKey(params, `labels:${targetLabels.map(normalizeKey).sort().join('|')}`);
  const existing = ENSURE_LABELS_INFLIGHT.get(key);
  if (existing) {
    await existing;
    return;
  }

  const pending = (async (): Promise<void> => {
    let currentLabels: string[] = [];

    try {
      currentLabels = await fetchIssueLabels(context, params);
    } catch {
      currentLabels = [];
    }

    const currentKeys = new Set(currentLabels.map(normalizeKey).filter(Boolean));
    const missing = targetLabels.filter((label) => {
      const key = normalizeKey(label);
      return key && !currentKeys.has(key);
    });

    if (!missing.length) return;

    try {
      await context.octokit.issues.addLabels({
        ...params,
        labels: missing,
      });
    } catch (error: unknown) {
      const status = getHttpStatus(error);
      if (status !== 404) {
        log(
          context,
          'warn',
          {
            err: getErrorMessage(error),
            labels: missing,
            issueNumber: params.issue_number,
          },
          'failed to ensure labels'
        );
      }
    }
  })().finally(() => {
    ENSURE_LABELS_INFLIGHT.delete(key);
  });

  ENSURE_LABELS_INFLIGHT.set(key, pending);
  await pending;
}

async function ensureAssigneesPresent(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  assignees: string[]
): Promise<void> {
  const targetAssignees = uniqLogins((assignees || []).map((x) => toStringTrim(x)).filter(Boolean));
  if (!targetAssignees.length) return;

  try {
    const { data } = await context.octokit.issues.get(params);
    const currentAssignees = uniqLogins(
      (((data as Record<string, unknown>)['assignees'] as (Record<string, unknown> | null | undefined)[]) || [])
        .map((item) => normalizeLogin(toStringTrim(item?.login)))
        .filter(Boolean)
    );

    const missing = targetAssignees.filter(
      (candidate) =>
        !currentAssignees.some(
          (existing) => normalizeLogin(existing).toLowerCase() === normalizeLogin(candidate).toLowerCase()
        )
    );

    if (!missing.length) return;

    await context.octokit.issues.addAssignees({
      ...params,
      assignees: missing,
    });
  } catch (e: unknown) {
    log(
      context,
      'warn',
      {
        err: e instanceof Error ? e.message : String(e),
        issueNumber: params.issue_number,
        assignees: targetAssignees,
      },
      'failed to ensure assignees'
    );
  }
}

function buildReviewHandoverBody(
  context: BotContext<RequestEvents>,
  snapshotHash?: string,
  options: { target?: 'issue' | 'pull_request' } = {}
): string {
  const docsLinks = getDocLinksFromConfig(context.resourceBotConfig ?? DEFAULT_CONFIG);
  return buildReviewHandoverBodyPure(docsLinks, snapshotHash, options);
}

function buildReviewHandoverOptions(): {
  resolveEffectiveConstants: (context: BotContext<RequestEvents>) => EffectiveConstants;
  resolveApproverRoutingForRequestType: (
    context: BotContext<RequestEvents>,
    requestType: string | undefined,
    approverUsernames: string[],
    approverPoolUsernames: string[]
  ) => ReturnType<typeof resolveApproverRoutingForRequestType>;
  pickAutoAssigneeFromPool: (issue: IssueLike, pool: string[]) => string[];
  uniqLogins: (logins: string[]) => string[];
  toStringTrim: (value: unknown) => string;
  ensureAssigneesPresent: (
    context: BotContext<RequestEvents>,
    params: IssueParams,
    assignees: string[]
  ) => Promise<void>;
  ensureLabelsPresentOnce: (context: BotContext<RequestEvents>, params: IssueParams, labels: string[]) => Promise<void>;
  buildReviewHandoverBody: (context: BotContext<RequestEvents>, snapshotHash?: string) => string;
} {
  return {
    resolveEffectiveConstants,
    resolveApproverRoutingForRequestType,
    pickAutoAssigneeFromPool,
    uniqLogins,
    toStringTrim,
    ensureAssigneesPresent,
    ensureLabelsPresentOnce,
    buildReviewHandoverBody,
  };
}

async function ensureReviewLabelsPresentOnIssue(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  eff: EffectiveConstants
): Promise<boolean> {
  const cfgKeys = (eff.reviewRequestedLabels || []).map(normalizeKey);
  if (!cfgKeys.length) return true;

  let labels = toLabelNames(issue.labels);

  try {
    labels = await fetchIssueLabels(context, params);
  } catch {
    // keep payload labels as fallback
  }

  return labels.some((l) => {
    const k = normalizeKey(l);
    return cfgKeys.some((ck) => k === ck || k.includes(ck) || ck.includes(k));
  });
}

async function removeReviewPendingLabelsAfterApproval(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  eff: EffectiveConstants
): Promise<void> {
  const approvedCfg = toStringTrim(eff.labelOnApproved);
  const pendingCfg = (eff.reviewRequestedLabels || []).map(toStringTrim).filter(Boolean);

  if (!approvedCfg || !pendingCfg.length) return;

  let labels: string[] = [];
  try {
    labels = await fetchIssueLabels(context, params);
  } catch {
    return;
  }

  const approvedKey = normalizeKey(approvedCfg);
  const hasApproved = labels.some((l) => {
    const k = normalizeKey(l);
    return k === approvedKey || k.includes(approvedKey) || approvedKey.includes(k);
  });

  if (!hasApproved) return;

  const pendingKeys = pendingCfg.map(normalizeKey);

  const toRemove = labels.filter((l) => {
    const k = normalizeKey(l);
    return pendingKeys.some((pk) => k === pk || k.includes(pk) || pk.includes(k));
  });

  for (const label of toRemove) {
    try {
      await context.octokit.issues.removeLabel({ ...params, name: label });
    } catch (e: unknown) {
      if (getHttpStatus(e) !== 404) {
        log(
          context,
          'warn',
          { err: e instanceof Error ? e.message : String(e), label },
          'failed to remove review pending label after approval'
        );
      }
    }
  }
}

// Request lifecycle status labels
const REQUEST_STATUS_LABEL_REQUESTER_ACTION = 'Requester Action';
const REQUEST_STATUS_LABEL_REVIEW_PENDING = 'Review Pending';
const REQUEST_STATUS_LABEL_PARENT_OWNER_ACTION = 'Parent Owner Action';
const REQUEST_STATUS_LABEL_REJECTED = 'Rejected';

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

function resolveParentOwnerActionLabel(context: BotContext<RequestEvents>): string {
  return resolveWorkflowLabel(context, 'parentOwnerAction', REQUEST_STATUS_LABEL_PARENT_OWNER_ACTION);
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
  const parentOwnerActionLabel = resolveParentOwnerActionLabel(context);

  let labels = (currentLabels || []).slice();
  if (!labels.length) {
    try {
      labels = await fetchIssueLabels(context, params);
    } catch {
      return;
    }
  }

  const toRemove = labelsMatching(labels, parentOwnerActionLabel);
  if (!toRemove.length) return;

  await removeExactLabelsFromIssue(context, params, toRemove);
}

async function setParentOwnerActionState(context: BotContext<RequestEvents>, params: IssueParams): Promise<void> {
  const eff = resolveEffectiveConstants(context);

  const parentOwnerActionLabel = resolveParentOwnerActionLabel(context);
  const authorActionLabel = resolveWorkflowLabel(context, 'authorAction', REQUEST_STATUS_LABEL_REQUESTER_ACTION);
  const approverActionLabel = resolveWorkflowLabel(context, 'approverAction', REQUEST_STATUS_LABEL_REVIEW_PENDING);
  const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';

  let labels: string[] = [];
  try {
    labels = await fetchIssueLabels(context, params);
  } catch {
    labels = [];
  }

  const toRemove = new Set<string>();

  for (const label of [
    authorActionLabel,
    approverActionLabel,
    approvedLabel,
    REQUEST_STATUS_LABEL_REJECTED,
    ...(eff.reviewRequestedLabels || []),
  ]) {
    for (const match of labelsMatching(labels, label)) {
      if (normalizeKey(match) !== normalizeKey(parentOwnerActionLabel)) {
        toRemove.add(match);
      }
    }
  }

  if (toRemove.size) {
    await removeExactLabelsFromIssue(context, params, Array.from(toRemove));
  }

  await ensureLabelsPresentOnce(context, params, [parentOwnerActionLabel]);
}

async function assignParentOwnersForApproval(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  owners: string[]
): Promise<void> {
  const assignees = uniqLogins((owners || []).map(toStringTrim).filter(Boolean));
  if (!assignees.length) return;

  // Best effort only.
  // If GitHub does not allow assignment, ensureAssigneesPresent logs and continues.
  // The parent owners are still mentioned in the comment as fallback.
  await ensureAssigneesPresent(context, params, assignees);
}

async function removeExactLabelsFromIssue(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  labelsToRemove: string[]
): Promise<void> {
  for (const label of labelsToRemove) {
    const name = toStringTrim(label);
    if (!name) continue;

    try {
      await context.octokit.issues.removeLabel({ ...params, name });
    } catch (e: unknown) {
      if (getHttpStatus(e) !== 404) {
        log(
          context,
          'warn',
          { err: e instanceof Error ? e.message : String(e), label: name },
          'failed to remove label'
        );
      }
    }
  }
}

async function removeProgressStatusLabels(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  currentLabels?: string[]
): Promise<void> {
  let labels = (currentLabels || []).slice();
  if (!labels.length) {
    try {
      labels = await fetchIssueLabels(context, params);
    } catch {
      return;
    }
  }

  const authorActionLabel = resolveWorkflowLabel(context, 'authorAction', REQUEST_STATUS_LABEL_REQUESTER_ACTION);
  const approverActionLabel = resolveWorkflowLabel(context, 'approverAction', REQUEST_STATUS_LABEL_REVIEW_PENDING);
  const parentOwnerActionLabel = resolveParentOwnerActionLabel(context);

  const toRemove = new Set<string>([
    ...labelsMatching(labels, authorActionLabel),
    ...labelsMatching(labels, approverActionLabel),
    ...labelsMatching(labels, parentOwnerActionLabel),
  ]);

  if (!toRemove.size) return;
  await removeExactLabelsFromIssue(context, params, Array.from(toRemove));
}

async function removeRejectedStatusLabel(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  currentLabels?: string[]
): Promise<void> {
  let labels = (currentLabels || []).slice();
  if (!labels.length) {
    try {
      labels = await fetchIssueLabels(context, params);
    } catch {
      return;
    }
  }

  const toRemove = labelsMatching(labels, REQUEST_STATUS_LABEL_REJECTED);
  if (!toRemove.length) return;
  await removeExactLabelsFromIssue(context, params, toRemove);
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
  try {
    if (eff.labelOnApproved) {
      await context.octokit.issues.addLabels({ ...params, labels: [eff.labelOnApproved] });
    }
  } catch {
    // ignore
  }

  await removeReviewPendingLabelsAfterApproval(context, params, eff);

  try {
    const labelsAfter = await fetchIssueLabels(context, params);
    const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';
    if (labelsMatching(labelsAfter, approvedLabel).length) {
      await removeProgressStatusLabels(context, params, labelsAfter);
      await removeRejectedStatusLabel(context, params, labelsAfter);
    }
  } catch {
    // ignore
  }
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
  const eff = resolveEffectiveConstants(context);
  const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';
  if (!approvedLabel) return;

  const params: IssueParams = {
    owner: repoInfo.owner,
    repo: repoInfo.repo,
    issue_number: prNumber,
  };

  try {
    await context.octokit.issues.addLabels({
      ...params,
      labels: [approvedLabel],
    });
  } catch {
    return;
  }

  // Standalone cross-repo direct PRs must stay PR-only here.
  // Reading the PR as an issue would break the no-linked-issue guarantee.
  if (options.skipStateCleanup) return;

  await removeReviewPendingLabelsAfterApproval(context, params, eff);

  try {
    const labelsAfter = await fetchIssueLabels(context, params);

    if (labelsMatching(labelsAfter, approvedLabel).length) {
      await removeProgressStatusLabels(context, params, labelsAfter);
      await removeRejectedStatusLabel(context, params, labelsAfter);
    }
  } catch {
    // best effort cleanup only
  }
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

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

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
    ): Promise<{ data?: { commit?: { sha?: string | null } } }> => await context.octokit.repos.getBranch(args),
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
    const res = await (
      context.octokit.pulls as unknown as {
        get: (args: { owner: string; repo: string; pull_number: number }) => Promise<{ data?: PullRequestLike }>;
      }
    ).get({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      pull_number: prNumber,
    });

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

function buildMergeApprovedPrOrUpdateBranchCallbacks(): MergeApprovedPrOrUpdateBranchCallbacks<
  BotContext<RequestEvents>,
  RepoInfo,
  PullRequestLike
> {
  return {
    waitForPullRequestMergeability,
    shouldUpdatePullRequestBranch,
    requestPullRequestBranchUpdateRespectingSequentialRegistryQueue,
    hasAutoApprovedPrHead,
    isPullRequestApprovedForBranchMaintenance,
    isCrossRepositoryPullRequest,
    evaluateHeadGreenForApprovalReevaluation,
    tryMergeIfGreen,
    readFreshPullRequest,
    log,
    getErrorMessage,
    getHttpStatus,
  };
}

async function tryMergeApprovedPrOrUpdateBranch(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  reason: string
): Promise<void> {
  await tryMergeApprovedPrOrUpdateBranchApplication(context, repoInfo, pr, reason, runMergeApprovedPrOrUpdateBranch);
}

async function runMergeApprovedPrOrUpdateBranch(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
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
    const { data } = await context.octokit.pulls.list({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      state: 'open',
      per_page: 100,
      page,
    });

    const prs = (data || []) as unknown as PullRequestLike[];
    if (!prs.length) break;

    out.push(...prs);

    if (prs.length < 100) break;
    page += 1;
    if (page > 20) break;
  }

  return out;
}

async function processPullRequestForAutoMerge(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike
): Promise<void> {
  const prBaseBranch = toStringTrim(pr.base?.ref);

  if (await isSequentialDirectRegistryPr(context, repoInfo, pr, prBaseBranch)) {
    if (await shouldDeferSequentialDirectRegistryPrProcessing(context, repoInfo, pr)) {
      return;
    }
  }

  const issueNumber = parseLinkedIssueNumberFromPr(pr, repoInfo);

  if (issueNumber === null) {
    const freshPr = (await readFreshPullRequest(context, repoInfo, pr.number)) || pr;
    const standaloneOutcome = await maybeHandleStandaloneDirectPrApproval(context, repoInfo, freshPr, {
      baseBranch: toStringTrim(freshPr.base?.ref),
    });

    if (standaloneOutcome !== 'approved') return;

    const approvedPr = (await readFreshPullRequest(context, repoInfo, freshPr.number)) || freshPr;
    await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, approvedPr, 'auto-merge');
    return;
  }

  const params: IssueParams = {
    owner: repoInfo.owner,
    repo: repoInfo.repo,
    issue_number: issueNumber,
  };

  let issue: IssueLike;
  try {
    const res = await context.octokit.issues.get(params);
    issue = res.data as unknown as IssueLike;
  } catch (error: unknown) {
    log(
      context,
      'warn',
      {
        prNumber: pr.number,
        issueNumber,
        err: getErrorMessage(error),
        status: getHttpStatus(error),
        crossRepo: isCrossRepositoryPullRequest(pr, repoInfo),
      },
      'direct-pr:linked-issue-read-failed-fallback-standalone'
    );

    const freshPr = (await readFreshPullRequest(context, repoInfo, pr.number)) || pr;
    const standaloneOutcome = await maybeHandleStandaloneDirectPrApproval(context, repoInfo, freshPr, {
      baseBranch: toStringTrim(freshPr.base?.ref),
    });

    if (standaloneOutcome !== 'approved') return;

    const approvedPr = (await readFreshPullRequest(context, repoInfo, freshPr.number)) || freshPr;
    await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, approvedPr, 'auto-merge');
    return;
  }

  if (!process.env.JEST_WORKER_ID && !hasIssueFormInputs(issue)) {
    log(
      context,
      'info',
      {
        prNumber: pr.number,
        issueNumber,
      },
      'direct-pr:linked-issue-not-request-form-fallback-standalone'
    );

    const standaloneOutcome = await maybeHandleStandaloneDirectPrApproval(context, repoInfo, pr);
    if (standaloneOutcome !== 'approved') return;

    await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
    return;
  }

  let template: TemplateLike;
  try {
    template = await loadTemplateWithLabelRefresh(context, params, issue);
  } catch (error: unknown) {
    log(
      context,
      'warn',
      {
        prNumber: pr.number,
        issueNumber,
        err: getErrorMessage(error),
        status: getHttpStatus(error),
      },
      'direct-pr:linked-issue-template-load-failed-fallback-standalone'
    );

    const standaloneOutcome = await maybeHandleStandaloneDirectPrApproval(context, repoInfo, pr);
    if (standaloneOutcome !== 'approved') return;

    await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
    return;
  }

  const parsedFormData = template ? parseForm(readIssueBodyForProcessing(issue.body), template) : {};
  if (!isRequestIssue(context, template, parsedFormData)) {
    log(
      context,
      'info',
      {
        prNumber: pr.number,
        issueNumber,
        parsedKeys: Object.keys(parsedFormData || {}),
      },
      'direct-pr:linked-issue-not-request-issue-fallback-standalone'
    );

    const standaloneOutcome = await maybeHandleStandaloneDirectPrApproval(context, repoInfo, pr);
    if (standaloneOutcome !== 'approved') return;

    await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
    return;
  }

  const body = toStringTrim(pr.body);
  const snapshotHashes = buildCompatibleRequestSnapshotHashes(issue.body, parsedFormData, template);
  const currentHash =
    snapshotHashes[0] || calcSnapshotHash(parsedFormData, template, readIssueBodyForProcessing(issue.body));
  const prHash = extractHashFromPrBody(body);

  if (prHash) {
    if (!snapshotHashes.includes(prHash)) {
      await closeOutdatedRequestPrs(context, params, template, {
        parsedFormData,
        currentHash,
        acceptedHashes: snapshotHashes,
      });
      return;
    }

    await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
    return;
  }

  const directPrOutcome = await maybeHandleDirectPrApprovalForMerge(
    context,
    repoInfo,
    params,
    issue,
    template,
    parsedFormData,
    pr
  );

  if (directPrOutcome !== 'approved') return;

  await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
}

function isApprovalConfigChangePath(filePath: string): boolean {
  return /^\.github\/registry-bot\/config\.(?:[cm]?js|ts|ya?ml)$/i.test(normalizeRepoPath(filePath));
}

function readPushChangedFiles(payload: unknown): string[] {
  if (!isPlainObject(payload)) return [];

  const commits = Array.isArray(payload['commits']) ? payload['commits'] : [];
  const out: string[] = [];
  const seen = new Set<string>();

  for (const commit of commits) {
    if (!isPlainObject(commit)) continue;

    for (const key of ['added', 'modified', 'removed'] as const) {
      const files = Array.isArray(commit[key]) ? commit[key] : [];
      for (const file of files) {
        const normalized = normalizeRepoPath(file);
        if (!normalized || seen.has(normalized)) continue;
        seen.add(normalized);
        out.push(normalized);
      }
    }
  }

  return out;
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

function isDefaultBranchPush(payload: unknown): boolean {
  if (!isPlainObject(payload)) return false;

  const ref = toStringTrim(payload['ref']);
  const repoObj = isPlainObject(payload['repository']) ? payload['repository'] : null;
  const defaultBranch = repoObj ? toStringTrim(repoObj['default_branch']) : '';

  return Boolean(ref && defaultBranch && ref === `refs/heads/${defaultBranch}`);
}

function readDefaultBranchFromPayload(payload: unknown): string {
  if (!isPlainObject(payload)) return '';

  const repoObj = isPlainObject(payload['repository']) ? payload['repository'] : null;
  return repoObj ? toStringTrim(repoObj['default_branch']) : '';
}

function readDefaultBranchFromPush(payload: unknown): string {
  return readDefaultBranchFromPayload(payload);
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
  const filename = normalizeRepoPath(file?.filename);
  const status = toStringTrim(file?.status).toLowerCase();

  if (!filename || !isYamlPath(filename) || status === 'removed') return '';
  return filename;
}

async function listChangedYamlFilesPage(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number,
  page: number
): Promise<PullRequestFileLike[]> {
  const res = await (
    context.octokit.pulls as unknown as {
      listFiles: (args: {
        owner: string;
        repo: string;
        pull_number: number;
        per_page?: number;
        page?: number;
      }) => Promise<{ data?: PullRequestFileLike[] }>;
    }
  ).listFiles({
    owner: repoInfo.owner,
    repo: repoInfo.repo,
    pull_number: prNumber,
    per_page: 100,
    page,
  });

  return Array.isArray(res?.data) ? res.data : [];
}

async function listChangedYamlFilesForPr(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number
): Promise<string[]> {
  const out: string[] = [];
  let page = 1;

  while (true) {
    const files = await listChangedYamlFilesPage(context, repoInfo, prNumber, page);
    if (!files.length) break;

    for (const file of files) {
      const filename = isChangedYamlCandidate(file);
      if (filename && isRegistryEntryPath(context, filename)) out.push(filename);
    }

    if (files.length < 100) break;
    page += 1;
    if (page > 20) break;
  }

  return Array.from(new Set(out));
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
  const treeSha = toStringTrim(ref);
  if (!treeSha) return [];

  try {
    const res = await (
      context.octokit.git as unknown as {
        getTree: (args: {
          owner: string;
          repo: string;
          tree_sha: string;
          recursive?: 'true';
        }) => Promise<{ data?: GitTreeLike }>;
      }
    ).getTree({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      tree_sha: treeSha,
      recursive: 'true',
    });

    return Array.isArray(res?.data?.tree) ? res.data.tree : [];
  } catch (error: unknown) {
    log(
      context,
      'warn',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        ref: treeSha,
        err: getErrorMessage(error),
        status: getHttpStatus(error),
      },
      'git-tree:read-failed'
    );

    return [];
  }
}

function registryYamlTreeEntryPath(context: BotContext<RequestEvents>, entry: GitTreeEntryLike): string {
  const path = normalizeRepoPath(entry?.path);
  const type = toStringTrim(entry?.type).toLowerCase();

  if (type !== 'blob') return '';
  if (!path || !isYamlPath(path)) return '';
  if (!isRegistryEntryPath(context, path)) return '';

  return path;
}

type PullRequestHeadReadCandidate = {
  repoInfo: RepoInfo;
  ref: string;
  source: string;
};

function sameRepoInfo(a: RepoInfo, b: RepoInfo): boolean {
  return sameRepoInfoPure(a, b);
}

function resolvePullRequestHeadRepoInfo(pr: PullRequestLike, fallbackRepoInfo: RepoInfo): RepoInfo {
  return resolvePullRequestHeadRepoInfoPure(pr, fallbackRepoInfo);
}

function isCrossRepositoryPullRequest(pr: PullRequestLike, baseRepoInfo: RepoInfo): boolean {
  return isCrossRepositoryPullRequestPure(pr, baseRepoInfo);
}

function buildPullRequestHeadReadCandidates(repoInfo: RepoInfo, pr: PullRequestLike): PullRequestHeadReadCandidate[] {
  const headRepoInfo = resolvePullRequestHeadRepoInfo(pr, repoInfo);
  const headSha = toStringTrim(pr.head?.sha);
  const headRef = toStringTrim(pr.head?.ref);
  const isCrossRepo = !sameRepoInfo(headRepoInfo, repoInfo);

  const out: PullRequestHeadReadCandidate[] = [];
  const seen = new Set<string>();

  const add = (candidateRepoInfo: RepoInfo, ref: string, source: string): void => {
    const normalizedRef = toStringTrim(ref);
    if (!normalizedRef) return;

    const key = `${candidateRepoInfo.owner}/${candidateRepoInfo.repo}:${normalizedRef}`;
    if (seen.has(key)) return;
    seen.add(key);

    out.push({
      repoInfo: candidateRepoInfo,
      ref: normalizedRef,
      source,
    });
  };

  add(repoInfo, headSha, 'base-repo:head-sha');
  add(repoInfo, `refs/pull/${pr.number}/head`, 'base-repo:pull-ref-full');
  add(repoInfo, `pull/${pr.number}/head`, 'base-repo:pull-ref-short');

  if (!isCrossRepo) {
    add(repoInfo, headRef, 'base-repo:head-ref');
  }

  add(headRepoInfo, headSha, 'head-repo:head-sha');
  add(headRepoInfo, headRef, 'head-repo:head-ref');

  return out;
}

async function readPullRequestHeadFileText(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  path: string
): Promise<string | null> {
  const candidates = buildPullRequestHeadReadCandidates(repoInfo, pr);
  const normalizedPath = normalizeRepoPath(path);

  for (const candidate of candidates) {
    const raw = await readRepoFileTextAtRef(context, candidate.repoInfo, normalizedPath, candidate.ref);
    if (raw === null) continue;

    log(
      context,
      'info',
      {
        prNumber: pr.number,
        path: normalizedPath,
        source: candidate.source,
        owner: candidate.repoInfo.owner,
        repo: candidate.repoInfo.repo,
        ref: candidate.ref,
        crossRepo: isCrossRepositoryPullRequest(pr, repoInfo),
      },
      'pull-request head file resolved'
    );

    return raw;
  }

  log(
    context,
    'warn',
    {
      prNumber: pr.number,
      path: normalizedPath,
      baseOwner: repoInfo.owner,
      baseRepo: repoInfo.repo,
      headOwner: resolvePullRequestHeadRepoInfo(pr, repoInfo).owner,
      headRepo: resolvePullRequestHeadRepoInfo(pr, repoInfo).repo,
      headRef: toStringTrim(pr.head?.ref),
      headSha: toStringTrim(pr.head?.sha),
      crossRepo: isCrossRepositoryPullRequest(pr, repoInfo),
      candidates: candidates.map((candidate) => ({
        source: candidate.source,
        owner: candidate.repoInfo.owner,
        repo: candidate.repoInfo.repo,
        ref: candidate.ref,
      })),
    },
    'pull-request head file read failed'
  );

  return null;
}

async function readPullRequestHeadTreeEntries(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike
): Promise<GitTreeEntryLike[]> {
  const headSha = toStringTrim(pr.head?.sha);
  if (!headSha) return [];

  const headRepoInfo = resolvePullRequestHeadRepoInfo(pr, repoInfo);
  const candidates: PullRequestHeadReadCandidate[] = [
    {
      repoInfo,
      ref: headSha,
      source: 'base-repo:head-sha',
    },
  ];

  if (!sameRepoInfo(headRepoInfo, repoInfo)) {
    candidates.push({
      repoInfo: headRepoInfo,
      ref: headSha,
      source: 'head-repo:head-sha',
    });
  }

  for (const candidate of candidates) {
    const entries = await readRecursiveGitTreeEntries(context, candidate.repoInfo, candidate.ref);
    if (!entries.length) continue;

    log(
      context,
      'info',
      {
        prNumber: pr.number,
        source: candidate.source,
        owner: candidate.repoInfo.owner,
        repo: candidate.repoInfo.repo,
        ref: candidate.ref,
        crossRepo: isCrossRepositoryPullRequest(pr, repoInfo),
        entries: entries.length,
      },
      'pull-request head tree resolved'
    );

    return entries;
  }

  return [];
}

async function listChangedYamlFilesForPrAgainstCurrentBase(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch: string
): Promise<string[]> {
  const baseRef = toStringTrim(baseBranch) || toStringTrim(pr.base?.ref);
  if (!baseRef) return [];

  const baseSha = await readBranchHeadSha(context, repoInfo, baseRef);
  if (!baseSha) return [];

  const [baseEntries, headEntries] = await Promise.all([
    readRecursiveGitTreeEntries(context, repoInfo, baseSha),
    readPullRequestHeadTreeEntries(context, repoInfo, pr),
  ]);

  const baseByPath = new Map<string, string>();

  for (const entry of baseEntries) {
    const path = registryYamlTreeEntryPath(context, entry);
    if (!path) continue;

    const sha = toStringTrim(entry.sha);
    if (sha) baseByPath.set(path, sha);
  }

  const changed: string[] = [];
  const seen = new Set<string>();

  for (const entry of headEntries) {
    const path = registryYamlTreeEntryPath(context, entry);
    if (!path || seen.has(path)) continue;

    const headEntrySha = toStringTrim(entry.sha);
    const baseEntrySha = baseByPath.get(path) || '';

    if (!headEntrySha) continue;
    if (baseEntrySha && baseEntrySha === headEntrySha) continue;

    seen.add(path);
    changed.push(path);
  }

  return changed;
}

async function listChangedYamlFilesForPrWithFallback(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch?: string
): Promise<string[]> {
  const fromPullFiles = await listChangedYamlFilesForPr(context, repoInfo, pr.number);
  if (fromPullFiles.length) return fromPullFiles;

  const fallbackBaseBranch = toStringTrim(baseBranch) || toStringTrim(pr.base?.ref);
  if (!fallbackBaseBranch) return [];

  const fromTreeDiff = await listChangedYamlFilesForPrAgainstCurrentBase(context, repoInfo, pr, fallbackBaseBranch);

  if (fromTreeDiff.length) {
    log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha: toStringTrim(pr.head?.sha),
        baseBranch: fallbackBaseBranch,
        changedRegistryFiles: fromTreeDiff,
      },
      'changed-registry-files:fallback-tree-diff'
    );
  }

  return fromTreeDiff;
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
  const targetBaseBranch = toStringTrim(baseBranch) || toStringTrim(pr.base?.ref);
  if (!targetBaseBranch) return false;
  if (isSnapshotManagedRequestPr(pr)) return false;
  if (!pullRequestTargetsBranch(pr, targetBaseBranch)) return false;

  try {
    const changedRegistryFiles = await listChangedYamlFilesForPrWithFallback(context, repoInfo, pr, targetBaseBranch);
    return changedRegistryFiles.length > 0;
  } catch (error) {
    log(
      context,
      'warn',
      {
        prNumber: pr.number,
        baseBranch: targetBaseBranch,
        error: error instanceof Error ? error.message : String(error),
      },
      'sequential-registry-pr:changed-files-lookup-failed'
    );

    return false;
  }
}

async function shouldDeferSequentialDirectRegistryPrProcessing(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike
): Promise<boolean> {
  const active = getSequentialRegistryPrActive(repoInfo);
  if (!active || active.prNumber === pr.number) return false;

  if (!(await isSequentialRegistryPrActiveBlocking(context, repoInfo))) {
    return false;
  }

  const currentActive = getSequentialRegistryPrActive(repoInfo);
  if (!currentActive || currentActive.prNumber === pr.number) {
    return false;
  }

  log(
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
  BotContext<RequestEvents>,
  RepoInfo,
  PullRequestLike,
  SequentialRegistryPrActiveState | null
> {
  return {
    isSequentialDirectRegistryPr,
    requestPullRequestBranchUpdate,
    getSequentialRegistryPrActive,
    markSequentialRegistryPrActive,
    runOneSequentialDirectRegistryPrMaintenance,
  };
}

async function requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
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
  BotContext<RequestEvents>,
  RepoInfo,
  PullRequestLike,
  SequentialRegistryPrActiveState | null,
  HeadGreenEvaluation
> {
  return {
    readFreshPullRequest,
    isPullRequestOpen,
    getSequentialRegistryPrActive,
    clearSequentialRegistryPrActive,
    markSequentialRegistryPrHeadSkipped,
    listOpenPullRequests,
    pullRequestTargetsBranch,
    listChangedYamlFilesForPrWithFallback,
    runOneSequentialDirectRegistryPrMaintenance,
    evaluateHeadGreenForApprovalReevaluation,
    isPullRequestApprovedForBranchMaintenance,
    log,
  };
}

async function advanceSequentialRegistryPrQueueAfterTerminalState(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
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
  BotContext<RequestEvents>,
  RepoInfo,
  PullRequestLike,
  HeadGreenEvaluation
> {
  return {
    isSequentialRegistryPrActiveBlocking,
    listOpenPullRequests,
    parseLinkedIssueNumberFromPr,
    isSnapshotManagedRequestPr,
    pullRequestTargetsBranch,
    isSequentialRegistryPrHeadSkipped,
    listChangedYamlFilesForPrWithFallback,
    readFreshPullRequest,
    shouldUpdatePullRequestBranch,
    isPullRequestApprovedForBranchMaintenance,
    requestPullRequestBranchUpdate,
    markSequentialRegistryPrActive,
    markSequentialRegistryPrHeadSkipped,
    evaluateHeadGreenForApprovalReevaluation,
    processPullRequestForAutoMerge,
    log,
  };
}

async function runOneSequentialDirectRegistryPrMaintenance(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  baseBranch: string,
  reason: string
): Promise<SequentialRegistryPrResult> {
  return await runOneSequentialDirectRegistryPrMaintenanceApplication(
    context,
    repoInfo,
    baseBranch,
    reason,
    buildSequentialRegistryPrQueueCallbacks()
  );
}

async function handleBlockingRegistryHeadConclusion(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
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
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike
): Promise<void> {
  await releaseSequentialRegistryPrIfNotApprovedAfterGreenApplication(
    context,
    repoInfo,
    pr,
    buildSequentialRegistryPrTerminalCallbacks()
  );
}

async function readRepoFileTextAtRef(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  path: string,
  ref: string
): Promise<string | null> {
  const p = normalizeRepoPath(path);
  const branchRef = toStringTrim(ref);
  if (!p || !branchRef) return null;

  try {
    const res = await (
      context.octokit.repos as unknown as {
        getContent: (args: { owner: string; repo: string; path: string; ref?: string }) => Promise<{ data?: unknown }>;
      }
    ).getContent({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      path: p,
      ref: branchRef,
    });

    const data = (res as { data?: unknown }).data;
    if (Array.isArray(data) || !isRepoContentFile(data)) return null;

    const enc = typeof data.encoding === 'string' ? data.encoding : 'base64';
    return Buffer.from(String(data.content || ''), enc as BufferEncoding).toString('utf8');
  } catch {
    return null;
  }
}

async function readRegistryDocForApproval(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  filePath: string
): Promise<Record<string, unknown> | null> {
  const raw = await readPullRequestHeadFileText(context, repoInfo, pr, filePath);
  if (!raw) return null;

  try {
    const parsed = YAML.parse(raw) as unknown;
    return isPlainObject(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function buildDefaultBranchApprovedPrBranchUpdateCallbacks(): DefaultBranchApprovedPrBranchUpdateCallbacks<
  BotContext<RequestEvents>,
  RepoInfo,
  PullRequestLike
> {
  return {
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
  };
}

function buildDefaultBranchDirectPrReevaluationCallbacks(): DefaultBranchDirectPrReevaluationCallbacks<
  BotContext<RequestEvents>,
  RepoInfo,
  SequentialRegistryPrResult
> {
  return {
    runOneSequentialDirectRegistryPrMaintenance,
    log,
  };
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
  return {
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
  };
}

function buildDirectPrApprovalCommentHandlingCallbacks(): DirectPrApprovalCommentHandlingCallbacks<
  BotContext<RequestEvents>,
  RepoInfo,
  IssueParams,
  PullRequestLike,
  IssueLike,
  EffectiveConstants
> {
  return {
    resolveEffectiveConstants,
    buildIssueParams: (repoInfo: RepoInfo, pr: PullRequestLike) => ({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      issue_number: pr.number,
    }),
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
  };
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

function buildApprovedRequestFinalizationCallbacks(): ApprovedRequestFinalizationCallbacks<
  BotContext<RequestEvents>,
  IssueParams,
  IssueLike,
  TemplateLike,
  FormData,
  EffectiveConstants,
  PullRequestLike
> {
  return {
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
  };
}

async function finalizeApprovedRequest(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData,
  options: {
    approvalPrefix?: string;
    approvalComment?: string;
    autoApproved?: boolean;
  }
): Promise<void> {
  await finalizeApprovedRequestApplication(
    context,
    params,
    issue,
    template,
    parsedFormData,
    options,
    buildApprovedRequestFinalizationCallbacks()
  );
}

function buildApprovalDecisionDispatchOptions(): {
  resolveApprovalDecision: (
    dispatchContext: BotContext<RequestEvents>,
    dispatchParams: IssueParams,
    dispatchIssue: IssueLike,
    dispatchTemplate: TemplateLike,
    dispatchFormData: FormData,
    dispatchRequestType: string,
    dispatchNamespace: string
  ) => Promise<ApprovalDecision | boolean>;
  handleApprovedDecision: (
    dispatchContext: BotContext<RequestEvents>,
    dispatchParams: IssueParams,
    dispatchIssue: IssueLike,
    dispatchTemplate: TemplateLike,
    dispatchFormData: FormData,
    decision: ApprovalDecision
  ) => Promise<void>;
  handleRejectedDecision: (
    dispatchContext: BotContext<RequestEvents>,
    dispatchParams: IssueParams,
    dispatchIssue: IssueLike,
    decision: ApprovalDecision
  ) => Promise<void>;
} {
  return {
    resolveApprovalDecision: (
      dispatchContext: BotContext<RequestEvents>,
      dispatchParams: IssueParams,
      dispatchIssue: IssueLike,
      dispatchTemplate: TemplateLike,
      dispatchFormData: FormData,
      dispatchRequestType: string,
      dispatchNamespace: string
    ): Promise<ApprovalDecision | boolean> =>
      runApprovalHook(
        dispatchContext,
        { owner: dispatchParams.owner, repo: dispatchParams.repo },
        {
          requestType: dispatchRequestType,
          namespace: dispatchNamespace,
          resourceName: extractResourceNameFromForm(dispatchFormData, dispatchTemplate),
          formData: dispatchFormData,
          issue: dispatchIssue,
        }
      ),
    handleApprovedDecision: (
      dispatchContext: BotContext<RequestEvents>,
      dispatchParams: IssueParams,
      dispatchIssue: IssueLike,
      dispatchTemplate: TemplateLike,
      dispatchFormData: FormData,
      decision: ApprovalDecision
    ): Promise<void> =>
      finalizeApprovedRequest(dispatchContext, dispatchParams, dispatchIssue, dispatchTemplate, dispatchFormData, {
        approvalPrefix: '',
        approvalComment: decision.comment,
        autoApproved: true,
      }),
    handleRejectedDecision: (
      dispatchContext: BotContext<RequestEvents>,
      dispatchParams: IssueParams,
      dispatchIssue: IssueLike,
      decision: ApprovalDecision
    ): Promise<void> =>
      rejectRequestFromApprovalHook(dispatchContext, dispatchParams, dispatchIssue, decision, {
        closeLinkedPrs: true,
        minimizeTag: undefined,
        listOpenPullRequests,
        parseLinkedIssueNumberFromPr,
      }),
  };
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

function resolveStructuredRootForTemplate(template: TemplateLike): string {
  return toStringTrim(template?._meta?.root).replace(/^\/+/, '').replace(/\/+$/, '');
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

function extractCreatePrFailureMessage(error: unknown): string {
  const raw = (error instanceof Error ? error.message : String(error)).trim();
  const withoutUrl = raw.replace(/\s*-\s*https?:\/\/\S+$/i, '').trim();

  const marker = 'Validation Failed:';
  const idx = withoutUrl.indexOf(marker);

  if (idx >= 0) {
    const tail = withoutUrl.slice(idx + marker.length).trim();

    try {
      const parsed = JSON.parse(tail) as Record<string, unknown>;
      const msg = toStringTrim(parsed['message']);
      if (msg) return msg;
    } catch {
      // ignore
    }

    return tail || withoutUrl;
  }

  return withoutUrl;
}

function parseNoCommitsHeadBranchFromCreatePrError(error: unknown): string {
  const raw = extractCreatePrFailureMessage(error);
  const m = /No commits between [^ ]+ and ([^"\s]+)/i.exec(raw);
  return m?.[1] ? toStringTrim(m[1]).replace(/^refs\/heads\//, '') : '';
}

function isResourceAlreadyExistsDuringPrCreation(error: unknown): boolean {
  const msg = extractCreatePrFailureMessage(error);
  return /Resource ['"`][^'"`]+['"`] already exists at /i.test(msg);
}

async function registryResourceExistsOnDefaultBranch(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  template: TemplateLike,
  resourceName: string
): Promise<boolean> {
  const structRoot = resolveStructuredRootForTemplate(template);
  if (!structRoot || !resourceName) return false;

  for (const ext of ['yaml', 'yml']) {
    try {
      await context.octokit.repos.getContent({
        owner: params.owner,
        repo: params.repo,
        path: `${structRoot}/${resourceName}.${ext}`,
      });
      return true;
    } catch (e: unknown) {
      if (getHttpStatus(e) === 404) continue;
      throw e;
    }
  }

  return false;
}

async function deleteBranchRefIfPresent(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  branchName: string
): Promise<void> {
  const branch = toStringTrim(branchName).replace(/^refs\/heads\//, '');
  if (!branch) return;

  try {
    await context.octokit.git.deleteRef({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      ref: `heads/${branch}`,
    });
  } catch (e: unknown) {
    if (getHttpStatus(e) !== 404) throw e;
  }
}

function formatCreateRequestFailureForUser(error: unknown, branchName = '', resourceName = ''): string {
  const msg = extractCreatePrFailureMessage(error);
  const parsedBranch = parseNoCommitsHeadBranchFromCreatePrError(error) || toStringTrim(branchName);

  if (/^No commits between\b/i.test(msg)) {
    const suffix = parsedBranch ? ` '${parsedBranch}'` : '';
    return `Failed to create PR automatically: stale request branch${suffix} blocked PR creation. Please retry approval.`;
  }

  if (isResourceAlreadyExistsDuringPrCreation(error)) {
    const suffix = resourceName ? ` '${resourceName}'` : '';
    return `Failed to create PR automatically: a stale request branch already contains${suffix}. Please retry approval.`;
  }

  return `Failed to create PR automatically: ${msg}`;
}

async function runCreateRequestPr(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  issue: IssueLike,
  parsedFormData: FormData,
  template: TemplateLike
): Promise<{ number: number }> {
  return await createRequestPr(context, repoInfo, issue, parsedFormData, { template });
}

async function retryCreatePrAfterBranchCleanup(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  branchName: string,
  issue: IssueLike,
  parsedFormData: FormData,
  template: TemplateLike
): Promise<{ number: number }> {
  await deleteBranchRefIfPresent(context, repoInfo, branchName);
  return await runCreateRequestPr(context, repoInfo, issue, parsedFormData, template);
}

async function handleNoCommitsCreatePrFailure(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  branchName: string,
  issue: IssueLike,
  parsedFormData: FormData,
  template: TemplateLike,
  resourceName: string
): Promise<{ number: number }> {
  try {
    return await retryCreatePrAfterBranchCleanup(context, repoInfo, branchName, issue, parsedFormData, template);
  } catch (retryError: unknown) {
    throw new Error(formatCreateRequestFailureForUser(retryError, branchName, resourceName));
  }
}

async function handleAlreadyExistsCreatePrFailure(
  context: BotContext<RequestEvents>,
  args: {
    params: IssueParams;
    repoInfo: RepoInfo;
    issue: IssueLike;
    parsedFormData: FormData;
    template: TemplateLike;
    resourceName: string;
    branchName: string;
  }
): Promise<{ number: number }> {
  const { params, repoInfo, issue, parsedFormData, template, resourceName, branchName } = args;

  try {
    const existsOnDefaultBranch = await registryResourceExistsOnDefaultBranch(context, params, template, resourceName);

    if (existsOnDefaultBranch) {
      throw new Error(`Failed to create PR automatically: Resource '${resourceName}' already exists in the registry.`);
    }

    return await retryCreatePrAfterBranchCleanup(context, repoInfo, branchName, issue, parsedFormData, template);
  } catch (retryError: unknown) {
    if (retryError instanceof Error && retryError.message.startsWith('Failed to create PR automatically:')) {
      throw retryError;
    }

    throw new Error(formatCreateRequestFailureForUser(retryError, branchName, resourceName));
  }
}

async function createRequestPrWithRecovery(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  parsedFormData: FormData,
  template: TemplateLike,
  resourceName: string
): Promise<{ number: number }> {
  const repoInfo: RepoInfo = { owner: params.owner, repo: params.repo };
  const fallbackBranchName = renderConfiguredRequestBranchName(context, issue, resourceName);

  try {
    return await runCreateRequestPr(context, repoInfo, issue, parsedFormData, template);
  } catch (error: unknown) {
    const staleNoCommitsBranch = parseNoCommitsHeadBranchFromCreatePrError(error) || fallbackBranchName;
    const failureMessage = extractCreatePrFailureMessage(error);

    if (/^No commits between\b/i.test(failureMessage)) {
      return await handleNoCommitsCreatePrFailure(
        context,
        repoInfo,
        staleNoCommitsBranch,
        issue,
        parsedFormData,
        template,
        resourceName
      );
    }

    if (isResourceAlreadyExistsDuringPrCreation(error)) {
      return await handleAlreadyExistsCreatePrFailure(context, {
        params,
        repoInfo,
        issue,
        parsedFormData,
        template,
        resourceName,
        branchName: fallbackBranchName,
      });
    }

    throw new Error(formatCreateRequestFailureForUser(error, staleNoCommitsBranch, resourceName));
  }
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
  return {
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
    onCloseOutdatedRequestPrsSkipped: (error: unknown): void => {
      (app.log || console).warn?.(
        { err: error instanceof Error ? error.message : String(error) },
        'closeOutdatedRequestPRs skipped'
      );
    },
    validateRequestIssue,
    checkParentChainExistsInFlatStructure,
    onParentChainCheckFailed: (error: unknown): void => {
      (app.log || console).warn?.(
        { err: error instanceof Error ? error.message : String(error) },
        'parent chain check failed'
      );
    },
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
  };
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
  return {
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
    onParentChainCheckFailed: (error: unknown): void => {
      (app.log || console).warn?.(
        { err: error instanceof Error ? error.message : String(error) },
        'parent chain check failed'
      );
    },
    onCloseOutdatedRequestPrsSkipped: (error: unknown): void => {
      (app.log || console).warn?.(
        { err: error instanceof Error ? error.message : String(error) },
        'closeOutdatedRequestPRs skipped'
      );
    },
    onRevalidationFailed: (error: unknown): void => {
      (app.log || console).warn?.(`Revalidation failed: ${error instanceof Error ? error.message : String(error)}`);
    },
  };
}

async function processIssueEvent(
  app: Probot,
  context: BotContext<'issues.opened' | 'issues.edited' | 'issues.reopened'>,
  params: IssueParams,
  issue: IssueLike
): Promise<void> {
  await processRequestIssueLifecycleApplication(context, params, issue, buildRequestIssueLifecycleCallbacks(app));
}

async function resolveAdditionalIssueApproversFromApprovalHook(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData,
  requestType?: string
): Promise<string[]> {
  const effectiveRequestType = requestType || resolveEffectiveRequestType(template, parsedFormData);
  const resourceName = extractResourceNameFromForm(parsedFormData, template);

  if (!effectiveRequestType) return [];

  try {
    const decision = normalizeApprovalDecision(
      await runApprovalHook(
        context,
        { owner: params.owner, repo: params.repo },
        {
          requestType: effectiveRequestType,
          namespace: resourceName,
          resourceName,
          formData: parsedFormData,
          issue,
          requestAuthorId: normalizeLogin(issue.user?.login),
        }
      )
    );

    return uniqLogins((decision.approvers || []).filter(Boolean));
  } catch {
    return [];
  }
}

async function resolveManualReviewApproverOverrideFromApprovalHook(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData,
  requestType?: string
): Promise<string[]> {
  const effectiveRequestType = requestType || resolveEffectiveRequestType(template, parsedFormData);
  const resourceName = extractResourceNameFromForm(parsedFormData, template);

  if (!effectiveRequestType) return [];

  try {
    const decision = normalizeApprovalDecision(
      await runApprovalHook(
        context,
        { owner: params.owner, repo: params.repo },
        {
          requestType: effectiveRequestType,
          namespace: resourceName,
          resourceName,
          formData: parsedFormData,
          issue,
          requestAuthorId: normalizeLogin(issue.user?.login),
        }
      )
    );

    return getUnknownManualApprovers(decision);
  } catch {
    return [];
  }
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
  return {
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
  };
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
  return {
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
    buildApprovedContactApprovalMeta: ({ target, owners, approvedBy, approvedAt }): ContactApprovalMeta => ({
      v: 1,
      target: toStringTrim(target),
      owners: uniqLogins(owners || []),
      approvedBy: normalizeLogin(approvedBy),
      approvedAt: toStringTrim(approvedAt),
    }),
    buildApprovedParentApprovalMeta: ({ parent, target, owners, approvedBy, approvedAt }): ParentApprovalMeta => ({
      v: 1,
      parent: toStringTrim(parent),
      target: toStringTrim(target),
      owners: uniqLogins(owners || []),
      approvedBy: normalizeLogin(approvedBy),
      approvedAt: toStringTrim(approvedAt),
    }),
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
  };
}

async function handleApprovalComment(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData,
  commenter: string
): Promise<void> {
  await handleApprovalCommentApplication(
    context,
    params,
    issue,
    template,
    parsedFormData,
    commenter,
    buildApprovalCommentHandlingCallbacks()
  );
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

async function repoYamlExists(context: BotContext<RequestEvents>, repo: RepoInfo, basePath: string): Promise<boolean> {
  for (const ext of ['yaml', 'yml']) {
    try {
      await context.octokit.repos.getContent({
        owner: repo.owner,
        repo: repo.repo,
        path: `${basePath}.${ext}`,
      });
      return true;
    } catch (e: unknown) {
      if (getHttpStatus(e) !== 404) throw e;
    }
  }

  return false;
}

async function checkParentChainExistsInFlatStructure(
  context: BotContext<RequestEvents>,
  { owner, repo }: RepoInfo,
  template: TemplateLike,
  formData: FormData,
  explicitResourceName?: string
): Promise<string | null> {
  const rootRaw = toStringTrim(template?._meta?.root);
  const structRoot = rootRaw.replace(/^\/+/, '').replace(/\/+$/, '');
  if (!structRoot) return null;

  const rt = toStringTrim(template?._meta?.requestType).toLowerCase();
  const isNamespaceLike = rt.includes('namespace') || rt === 'subcontext' || rt === 'system' || rt === 'authority';
  if (!isNamespaceLike) return null;

  const resourceName = toStringTrim(explicitResourceName) || extractResourceNameFromForm(formData, template);
  const parts = toStringTrim(resourceName).split('.').filter(Boolean);
  if (parts.length < 2) return null;

  const repoInfo: RepoInfo = { owner, repo };
  const vendorRoot = resolveVendorRegistryRootForRequestHandler(context);

  for (let i = parts.length - 1; i >= 1; i -= 1) {
    const parentName = parts.slice(0, i).join('.');
    if (!parentName) continue;

    const exists =
      i === 1
        ? await repoYamlExists(context, repoInfo, `${vendorRoot}/${parentName}`)
        : await repoYamlExists(context, repoInfo, `${structRoot}/${parentName}`);

    if (exists) continue;

    return i === 1
      ? `Vendor '${parentName}' is not present. Please register the vendor first.`
      : `Parent resource '${parentName}' is not present. Please register the parent first.`;
  }

  return null;
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

function readPayloadLabelName(payload: unknown): string {
  if (!isPlainObject(payload)) return '';
  const l = payload['label'];
  if (typeof l === 'string') return toStringTrim(l);
  if (isPlainObject(l)) return toStringTrim(l['name']);
  return '';
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

function sameNormalizedLoginSet(a: string[], b: string[]): boolean {
  const normalizedA = uniqLogins(a)
    .map((login) => normalizeLogin(login))
    .filter((login): login is string => !!login)
    .sort((left, right) => left.localeCompare(right));
  const normalizedB = uniqLogins(b)
    .map((login) => normalizeLogin(login))
    .filter((login): login is string => !!login)
    .sort((left, right) => left.localeCompare(right));

  if (normalizedA.length !== normalizedB.length) return false;
  return normalizedA.every((login, index) => login === normalizedB[index]);
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
  const current = readContactApprovalMeta(issue.body);

  if (!meta) {
    if (!current) return false;

    try {
      const nextBody = buildContactApprovalBody(issue.body, null);
      await context.octokit.issues.update({ ...params, body: nextBody });
      issue.body = nextBody;
      return true;
    } catch {
      return false;
    }
  }

  const next: ContactApprovalMeta = {
    v: 1,
    target: toStringTrim(meta.target),
    owners: uniqLogins(meta.owners || []),
  };

  const approvedBy = normalizeLogin(meta.approvedBy);
  const approvedAt = toStringTrim(meta.approvedAt);

  if (approvedBy) next.approvedBy = approvedBy;
  if (approvedAt) next.approvedAt = approvedAt;

  if (!next.target || !next.owners.length) return false;

  const same =
    current &&
    normalizeKey(current.target) === normalizeKey(next.target) &&
    sameNormalizedLoginSet(current.owners, next.owners) &&
    normalizeLogin(current.approvedBy) === normalizeLogin(next.approvedBy) &&
    toStringTrim(current.approvedAt) === toStringTrim(next.approvedAt);

  if (same) return false;

  const nextBody = buildContactApprovalBody(issue.body, next);

  try {
    await context.octokit.issues.update({ ...params, body: nextBody });
    issue.body = nextBody;
    return true;
  } catch {
    return false;
  }
}

async function resolveRequestContactOwnerLogins(
  context: BotContext<RequestEvents>,
  formData: FormData
): Promise<string[]> {
  const contacts = formData['contact'] ?? formData['contacts'] ?? '';
  const { logins: directLogins, emails } = extractParentContactCandidates(contacts);

  const resolved: string[] = [...directLogins];
  for (const email of emails.slice(0, 10)) {
    resolved.push(...(await lookupGithubLoginsByEmail(context, email)));
  }

  return uniqLogins(resolved);
}

async function maybeRequireSystemContactOwnerApproval(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  parsedFormData: FormData,
  requestType: string,
  validatedNamespace: string
): Promise<boolean> {
  if (normalizeTypeToken(requestType) !== 'systemnamespace') {
    await ensureContactApprovalMarker(context, params, issue, null);
    return false;
  }

  const target = toStringTrim(validatedNamespace);
  const owners = await resolveRequestContactOwnerLogins(context, parsedFormData);
  const requester = normalizeLogin(issue.user?.login);

  if (!target || !owners.length) {
    await ensureContactApprovalMarker(context, params, issue, null);
    return false;
  }

  if (requester && owners.some((owner) => owner.toLowerCase() === requester.toLowerCase())) {
    await ensureContactApprovalMarker(context, params, issue, null);
    return false;
  }

  const current = readContactApprovalMeta(issue.body);
  const alreadyApproved =
    current &&
    normalizeKey(current.target) === normalizeKey(target) &&
    sameNormalizedLoginSet(current.owners, owners) &&
    Boolean(normalizeLogin(current.approvedBy));

  if (alreadyApproved) return false;

  await ensureContactApprovalMarker(context, params, issue, { v: 1, target, owners });

  const mentions = owners.map((owner) => `@${owner}`).join(' ');
  const tag = `nsreq:contact-approval:${normalizeKey(target)}`;

  await postOnce(
    context,
    params,
    `### 🔒 Contact owner approval required

Requester @${requester || 'unknown'} is not listed in the contact owners for \`${target}\`.

${mentions}

Please confirm by commenting \`Approved\`. After that, the bot will continue with the standard review workflow.`,
    { minimizeTag: tag }
  );

  await setStateLabel(context, params, issue, 'author');
  return true;
}

async function handleSystemContactOwnerApprovalIfNeeded(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData,
  commenter: string
): Promise<boolean> {
  return await handleSystemContactOwnerApprovalIfNeededApplication(
    context,
    params,
    issue,
    template,
    parsedFormData,
    commenter,
    buildOwnerApprovalCommentHandlingCallbacks()
  );
}

async function ensureParentApprovalMarker(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  meta: ParentApprovalMeta | null
): Promise<boolean> {
  const current = readParentApprovalMeta(issue.body);

  if (!meta) {
    if (!current) return false;
    try {
      const nextBody = buildParentApprovalBody(issue.body, null);
      await context.octokit.issues.update({ ...params, body: nextBody });
      issue.body = nextBody;
      return true;
    } catch {
      return false;
    }
  }

  const next: ParentApprovalMeta = {
    v: 1,
    parent: toStringTrim(meta.parent),
    target: toStringTrim(meta.target),
    owners: uniqLogins(meta.owners || []),
  };

  const ab = normalizeLogin(meta.approvedBy);
  const at = toStringTrim(meta.approvedAt);
  if (ab) next.approvedBy = ab;
  if (at) next.approvedAt = at;

  if (!next.parent || !next.target) return false;

  const same =
    current &&
    normalizeKey(current.parent) === normalizeKey(next.parent) &&
    normalizeKey(current.target) === normalizeKey(next.target) &&
    uniqLogins(current.owners).join('|').toLowerCase() === uniqLogins(next.owners).join('|').toLowerCase() &&
    normalizeLogin(current.approvedBy) === normalizeLogin(next.approvedBy) &&
    toStringTrim(current.approvedAt) === toStringTrim(next.approvedAt);

  if (same) return false;

  const nextBody = buildParentApprovalBody(issue.body, next);

  try {
    await context.octokit.issues.update({ ...params, body: nextBody });
    issue.body = nextBody;
    return true;
  } catch {
    return false;
  }
}

async function maybeRequireParentOwnerApproval(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  validatedNamespace: string,
  requestType: string
): Promise<boolean> {
  const rt = toStringTrim(requestType).toLowerCase();
  if (!rt.includes('namespace')) {
    await ensureParentApprovalMarker(context, params, issue, null);
    await clearParentOwnerActionState(context, params);
    return false;
  }

  const target = toStringTrim(validatedNamespace);
  const parts = target
    .split('.')
    .map((p) => p.trim())
    .filter(Boolean);

  if (parts.length <= 2) {
    await ensureParentApprovalMarker(context, params, issue, null);
    await clearParentOwnerActionState(context, params);
    return false;
  }

  const requester = normalizeLogin(issue.user?.login);

  const { parent, owners } = await resolveParentOwnerLoginsForTarget(context, params, template, target, requestType);

  if (!parent || owners.length === 0) {
    await ensureParentApprovalMarker(context, params, issue, null);
    await clearParentOwnerActionState(context, params);
    return false;
  }

  if (requester && owners.some((o) => o.toLowerCase() === requester.toLowerCase())) {
    if (DBG) {
      log(
        context,
        'debug',
        { issue: issue.number, requester, parent, target, owners },
        'parent-approval:skip (requester is parent owner)'
      );
    }
    await ensureParentApprovalMarker(context, params, issue, null);
    await clearParentOwnerActionState(context, params);
    return false;
  }

  const current = readParentApprovalMeta(issue.body);
  const alreadyApproved =
    current &&
    normalizeKey(current.parent) === normalizeKey(parent) &&
    normalizeKey(current.target) === normalizeKey(target) &&
    Boolean(normalizeLogin(current.approvedBy));

  if (alreadyApproved) {
    await clearParentOwnerActionState(context, params);
    return false;
  }

  await ensureParentApprovalMarker(context, params, issue, { v: 1, parent, target, owners });

  await setParentOwnerActionState(context, params);
  await assignParentOwnersForApproval(context, params, owners);

  const mentions = owners.map((o) => `@${o}`).join(' ');
  const tag = `nsreq:parent-approval:${normalizeKey(parent)}:${normalizeKey(target)}`;

  await postOnce(
    context,
    params,
    `### 🔒 Parent owner approval required

Sub-namespace request under \`${parent}\` (target: \`${target}\`).

${mentions}

Please confirm by commenting \`Approved\`. After that, the bot will continue with the standard review workflow.`,
    { minimizeTag: tag }
  );

  return true;
}

async function handleParentOwnerApprovalIfNeeded(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData,
  commenter: string
): Promise<boolean> {
  return await handleParentOwnerApprovalIfNeededApplication(
    context,
    params,
    issue,
    template,
    parsedFormData,
    commenter,
    buildOwnerApprovalCommentHandlingCallbacks()
  );
}

const ROUTING_LOCK_NOTICE_INFLIGHT = new Map<string, Promise<void>>();

function routingNoticeKey(params: IssueParams): string {
  return `${params.owner}/${params.repo}#${params.issue_number}`;
}

async function postRoutingLockNoticeOnce(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  expected: string
): Promise<void> {
  const key = routingNoticeKey(params);
  const existing = ROUTING_LOCK_NOTICE_INFLIGHT.get(key);
  if (existing) {
    await existing;
    return;
  }

  const p = Promise.resolve()
    .then(async (): Promise<void> => {
      await postOnce(context, params, `Routing label is locked to "${expected}". Manual changes were reverted.`, {
        minimizeTag: 'nsreq:routing-label-lock',
      });
    })
    .finally(() => {
      ROUTING_LOCK_NOTICE_INFLIGHT.delete(key);
    });

  ROUTING_LOCK_NOTICE_INFLIGHT.set(key, p);
  await p;
}

async function isRoutingLabelName(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  labelName: unknown
): Promise<boolean> {
  const name = toStringTrim(labelName);
  if (!name) return false;
  try {
    return Boolean(await tryLoadTemplateForLabels(context, params, issue, [name]));
  } catch {
    return false;
  }
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

async function detectRoutingLabels(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  labels: string[]
): Promise<string[]> {
  const uniq: string[] = [];
  const seen = new Set<string>();
  for (const l of labels) {
    const name = toStringTrim(l);
    const key = normalizeKey(name);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    uniq.push(name);
  }

  const routing: string[] = [];
  for (const l of uniq) {
    const tpl = await tryLoadTemplateForLabels(context, params, issue, [l]);
    if (tpl) routing.push(l);
  }
  return routing;
}

async function detectSingleRoutingLabel(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  labels: string[]
): Promise<string> {
  const routing = await detectRoutingLabels(context, params, issue, labels);
  return routing.length === 1 ? routing[0] : '';
}

async function ensureRoutingLockMarker(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  expectedLabel: string
): Promise<boolean> {
  const expected = toStringTrim(expectedLabel);
  if (!expected) return false;

  const current = readRoutingLockExpected(issue.body);
  if (normalizeKey(current) === normalizeKey(expected)) return false;

  const nextBody = buildRoutingLockBody(issue.body, expected);

  try {
    await context.octokit.issues.update({ ...params, body: nextBody });
    return true;
  } catch {
    return false;
  }
}

async function enforceRoutingLabelLock(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  expectedLabel: string,
  opts?: { changedLabel?: string }
): Promise<boolean> {
  const expected = toStringTrim(expectedLabel);
  const expectedKey = normalizeKey(expected);
  if (!expectedKey) return false;

  let labels: string[] = [];
  try {
    labels = await fetchIssueLabels(context, params);
  } catch {
    labels = toLabelNames(issue.labels);
  }

  const routingLabels = await detectRoutingLabels(context, params, issue, labels);
  const toRemove = routingLabels.filter((l) => normalizeKey(l) !== expectedKey);

  const hasExpected = labels.some((l) => normalizeKey(l) === expectedKey);

  let changed = false;

  if (toRemove.length) {
    await removeExactLabelsFromIssue(context, params, toRemove);
    changed = true;
  }

  if (!hasExpected) {
    try {
      await context.octokit.issues.addLabels({ ...params, labels: [expected] });
      changed = true;
    } catch {
      // ignore label add errors
    }
  }

  if (changed) {
    const touchedLabel = toStringTrim(opts?.changedLabel);

    // Only notify on routing-label events - avoid spamming on unrelated label changes.
    const shouldNotify =
      !touchedLabel ||
      normalizeKey(touchedLabel) === expectedKey ||
      (await isRoutingLabelName(context, params, issue, touchedLabel));

    if (shouldNotify) {
      await postRoutingLockNoticeOnce(context, params, expected);
    }
  }

  return changed;
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

    await context.octokit.issues.update({
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
  { owner, repo, issue_number }: IssueParams,
  template: TemplateLike,
  options: { parsedFormData?: FormData; currentHash?: string; acceptedHashes?: string[] } = {}
): Promise<void> {
  const ensureFormAndHash = async (): Promise<{
    parsedFormData: FormData;
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

    const { data } = await context.octokit.issues.get({ owner, repo, issue_number });
    const issue = data as unknown as IssueLike;
    const bodyStr = readIssueBodyForProcessing(issue.body);
    const form = parseForm(bodyStr, template);
    const acceptedHashes = buildCompatibleRequestSnapshotHashes(issue.body, form, template);
    const currentHash = acceptedHashes[0] || calcSnapshotHash(form, template, bodyStr);

    return {
      parsedFormData: form,
      currentHash,
      acceptedHashes,
    };
  };

  const closePr = async (prNum: number, ref: string): Promise<void> => {
    try {
      await context.octokit.pulls.update({ owner, repo, pull_number: prNum, state: 'closed' });
    } catch {
      /* empty */
    }
    try {
      await context.octokit.git.deleteRef({ owner, repo, ref: `heads/${ref}` });
    } catch {
      /* empty */
    }
  };

  const { currentHash, acceptedHashes } = await ensureFormAndHash();
  const acceptedHashSet = new Set((acceptedHashes || []).map((value) => toStringTrim(value)).filter(Boolean));

  if (currentHash) acceptedHashSet.add(currentHash);

  const prs = await findOpenIssuePrs(context, { owner, repo }, issue_number);
  if (!prs.length) return;

  const eff = resolveEffectiveConstants(context);
  const onApproved = eff.labelOnApproved;
  const closed: number[] = [];

  for (const pr of prs) {
    const prHash = extractHashFromPrBody(toStringTrim(pr.body));

    if (!prHash) continue;
    if (acceptedHashSet.has(prHash)) continue;

    await closePr(pr.number, pr.head.ref);
    closed.push(pr.number);
  }

  if (!closed.length) return;

  const list = closed.map((n) => `#${n}`).join(', ');
  await postOnce(
    context,
    { owner, repo, issue_number },
    `Form updated → closing outdated PR(s): ${list}. Please re-approve to open a new PR.`,
    { minimizeTag: 'nsreq:pr-outdated' }
  );

  if (!onApproved) return;
  try {
    await context.octokit.issues.removeLabel({ owner, repo, issue_number, name: onApproved });
  } catch {
    /* empty */
  }
}

function readRepoInfoFromPayload(payload: unknown): RepoInfo | null {
  if (!isPlainObject(payload)) return null;

  const repoObj = payload['repository'];
  if (!isPlainObject(repoObj)) return null;

  const repoName = toStringTrim(repoObj['name']);
  const ownerObj = isPlainObject(repoObj['owner']) ? repoObj['owner'] : null;
  const ownerLogin = ownerObj ? toStringTrim(ownerObj['login']) : '';

  if (!ownerLogin || !repoName) return null;

  return { owner: ownerLogin, repo: repoName };
}

export default function requestHandler(app: Probot): void {
  const getStaticConfig = async (
    context: BotContext<RequestEvents>,
    options: StaticConfigLoadOptions = {}
  ): Promise<NormalizedStaticConfig> => {
    const forceReload = options.forceReload === true;

    if (!forceReload && context.resourceBotConfig && context.resourceBotHooks !== undefined) {
      return context.resourceBotConfig;
    }

    try {
      const { config, hooks, hooksSource } = await loadStaticConfig(context, {
        validate: false,
        updateIssue: false,
        forceReload,
      });

      context.resourceBotConfig = config;
      context.resourceBotHooks = hooks;
      context.resourceBotHooksSource = hooksSource || null;

      log(
        context,
        'info',
        {
          forceReload,
          hooksSource: context.resourceBotHooksSource,
        },
        'static-config:context-loaded'
      );

      return context.resourceBotConfig;
    } catch (err: unknown) {
      (app.log || console).warn?.(
        {
          err: err instanceof Error ? err.message : String(err),
          forceReload,
        },
        'failed to load resource-bot static config, using defaults'
      );

      context.resourceBotConfig = DEFAULT_CONFIG;
      context.resourceBotHooks = null;
      context.resourceBotHooksSource = null;

      return context.resourceBotConfig;
    }
  };

  const buildDefaultBranchCheckSuiteReevaluationCallbacks = (): DefaultBranchCheckSuiteReevaluationCallbacks<
    BotContext<RequestEvents>,
    RepoInfo,
    SequentialRegistryPrResult
  > => ({
    readDefaultBranchFromPayload,
    getBranch: async (
      context: BotContext<RequestEvents>,
      args: { owner: string; repo: string; branch: string }
    ): Promise<{ data?: { commit?: { sha?: string | null } } }> => await context.octokit.repos.getBranch(args),
    logBranchHeadReadFailed: (
      context: BotContext<RequestEvents>,
      args: {
        repoInfo: RepoInfo;
        defaultBranch: string;
        headSha: string;
        errorMessage: string;
        status: number | undefined;
      }
    ): void => {
      log(
        context,
        'warn',
        {
          owner: args.repoInfo.owner,
          repo: args.repoInfo.repo,
          defaultBranch: args.defaultBranch,
          headSha: args.headSha,
          err: args.errorMessage,
          status: args.status,
        },
        'default-branch-check-suite:branch-head-read-failed'
      );
    },
    logEvaluated: (
      context: BotContext<RequestEvents>,
      args: {
        repoInfo: RepoInfo;
        defaultBranch: string;
        headBranch: string;
        headSha: string;
        defaultBranchHeadSha: string;
        conclusion: string;
        status: string;
        isDefaultBranchSuite: boolean;
      }
    ): void => {
      log(
        context,
        'info',
        {
          owner: args.repoInfo.owner,
          repo: args.repoInfo.repo,
          defaultBranch: args.defaultBranch,
          headBranch: args.headBranch,
          headSha: args.headSha,
          defaultBranchHeadSha: args.defaultBranchHeadSha,
          conclusion: args.conclusion,
          status: args.status,
          isDefaultBranchSuite: args.isDefaultBranchSuite,
        },
        'default-branch-check-suite:evaluated'
      );
    },
    getErrorMessage,
    getHttpStatus,
    getStaticConfig: async (context: BotContext<RequestEvents>, options: { forceReload: true }): Promise<unknown> =>
      await getStaticConfig(context, options),
    reevaluateOpenDirectPullRequestsAfterDefaultBranchPush,
    updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry,
  });

  const buildAutoMergeTriggerCallbacks = (): AutoMergeTriggerCallbacks<
    BotContext<RequestEvents>,
    RepoInfo,
    PullRequestLike,
    SequentialRegistryPrActiveState | null,
    HeadGreenEvaluation
  > => ({
    getStaticConfig,
    evaluateHeadGreenForApprovalReevaluation,
    listOpenPullRequests,
    processPullRequestForAutoMerge,
    releaseSequentialRegistryPrIfNotApprovedAfterGreen,
    advanceSequentialRegistryPrQueueAfterTerminalState,
    readFreshPullRequest,
    isSequentialDirectRegistryPr,
    getSequentialRegistryPrActive,
    clearSequentialRegistryPrActive,
    markSequentialRegistryPrHeadSkipped,
    runOneSequentialDirectRegistryPrMaintenance,
    log,
  });

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
  app.on(
    ['issues.opened', 'issues.edited', 'issues.reopened'],
    async (context: BotContext<'issues.opened' | 'issues.edited' | 'issues.reopened'>): Promise<void> => {
      await getStaticConfig(context);

      if (shouldSkipIssueEditedEvent(context)) return;

      const sender = context.payload.sender as unknown as SenderLike;
      const action = toStringTrim((context.payload as unknown as Record<string, unknown>)['action']).toLowerCase();
      if (action === 'edited' && isBotSender(sender)) return; // prevent loops

      const issue = context.payload.issue as unknown as IssueLike;

      if (DBG) {
        const safeLabels = toLabelNames(issue?.labels);
        const payload = context.payload as unknown;

        let changesKeys: string[] = [];
        if (isPlainObject(payload) && 'changes' in payload) {
          const c = payload['changes'];
          if (isPlainObject(c)) changesKeys = Object.keys(c);
        }

        log(
          context,
          'debug',
          {
            action: (context.payload as unknown as Record<string, unknown>)?.action,
            issueNumber: issue?.number,
            issueId: issue?.id,
            title: issue?.title,
            state: issue?.state,
            user: issue?.user?.login,
            created_at: issue?.created_at,
            updated_at: issue?.updated_at,
            labels: safeLabels,
            bodyLen: String(issue?.body || '').length,
            bodyHead: String(issue?.body || '').slice(0, 300),
            changesKeys,
          },
          'dbg:issues:payload.issue'
        );
      }

      const { owner, repo, issue_number: issueNumber } = context.issue() as IssueParams;
      const params: IssueParams = { owner, repo, issue_number: issueNumber };
      await processIssueEvent(app, context, params, issue);
    }
  );

  app.on('issues.closed', async (context: BotContext<'issues.closed'>): Promise<void> => {
    await getStaticConfig(context);

    const issue = context.payload.issue as unknown as IssueLike;

    if (!process.env.JEST_WORKER_ID) {
      if (!hasIssueFormInputs(issue)) return;
    }

    const { owner, repo, issue_number: issueNumber } = context.issue() as IssueParams;
    const params: IssueParams = { owner, repo, issue_number: issueNumber };

    let template: TemplateLike;
    try {
      template = await loadTemplateWithLabelRefresh(context, params, issue);
    } catch {
      // Not a request issue
      return;
    }

    const parsedFormData = template ? parseForm(readIssueBodyForProcessing(issue.body), template) : {};
    if (!isRequestIssue(context, template, parsedFormData)) return;

    const eff = resolveEffectiveConstants(context);
    const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';

    let labels: string[] = [];
    try {
      labels = await fetchIssueLabels(context, params);
    } catch {
      labels = toLabelNames(issue.labels);
    }

    const hasApproved = labelsMatching(labels, approvedLabel).length > 0;

    // If approved, keep it clean
    if (hasApproved) {
      await removeRejectedStatusLabel(context, params, labels);
      await removeProgressStatusLabels(context, params, labels);
      return;
    }

    // Closed but not approved -> mark as rejected
    const hasRejected = labelsMatching(labels, REQUEST_STATUS_LABEL_REJECTED).length > 0;
    if (!hasRejected) {
      try {
        await context.octokit.issues.addLabels({
          ...params,
          labels: [REQUEST_STATUS_LABEL_REJECTED],
        });
      } catch (e: unknown) {
        log(
          context,
          'warn',
          { err: e instanceof Error ? e.message : String(e), label: REQUEST_STATUS_LABEL_REJECTED },
          'failed to add rejected status label'
        );
      }
    }

    // Clean up progress status labels once Rejected is present.
    try {
      labels = await fetchIssueLabels(context, params);
    } catch {
      // best effort
    }

    if (labelsMatching(labels, REQUEST_STATUS_LABEL_REJECTED).length) {
      await removeProgressStatusLabels(context, params, labels);

      // enforce mutual exclusivity
      const approvedMatches = labelsMatching(labels, approvedLabel);
      if (approvedMatches.length) {
        await removeExactLabelsFromIssue(context, params, approvedMatches);
      }
    }
  });

  app.on(
    ['issues.labeled', 'issues.unlabeled'],
    async (context: BotContext<'issues.labeled' | 'issues.unlabeled'>): Promise<void> => {
      await getStaticConfig(context);

      const sender = context.payload.sender as unknown as SenderLike;
      if (isBotSender(sender)) return; // prevent loops

      const issue = context.payload.issue as unknown as IssueLike;

      if (!process.env.JEST_WORKER_ID) {
        if (!hasIssueFormInputs(issue)) return;
      }
      const action = toStringTrim((context.payload as unknown as Record<string, unknown>)['action']).toLowerCase();

      const changedLabel = readPayloadLabelName(context.payload as unknown);
      if (!changedLabel) return;

      const { owner, repo, issue_number: issueNumber } = context.issue() as IssueParams;
      const params: IssueParams = { owner, repo, issue_number: issueNumber };

      let labels = toLabelNames(issue.labels);

      const expectedRouting = readRoutingLockExpected(issue.body);
      const hasRoutingLock = Boolean(expectedRouting);

      // Enforce routing label lock. This closes swap/multi-label bypasses.
      if (expectedRouting) {
        const enforced = await enforceRoutingLabelLock(context, params, issue, expectedRouting, { changedLabel });
        if (enforced) {
          try {
            labels = await fetchIssueLabels(context, params);
          } catch {
            // ignore
          }
        }
      }

      // 2) Load template
      let template: TemplateLike | null = null;
      let parsedFormData: FormData = {};

      try {
        template = await loadTemplateWithLabelRefresh(context, params, issue);
        parsedFormData = template ? parseForm(readIssueBodyForProcessing(issue.body), template) : {};
      } catch {
        if (!hasRoutingLock) return;
      }

      if (!hasRoutingLock && !isRequestIssue(context, template, parsedFormData)) return;

      const eff = resolveEffectiveConstants(context);

      // Allow manual switching of progress-state labels (authorAction / approverAction).
      const cfg: NormalizedStaticConfig = context.resourceBotConfig ?? DEFAULT_CONFIG;
      const wf = cfg?.workflow ?? {};
      const labelsCfg =
        isPlainObject(wf) && isPlainObject((wf as Record<string, unknown>)['labels'])
          ? ((wf as Record<string, unknown>)['labels'] as Record<string, unknown>)
          : {};

      const authorActionLabel = toStringTrim(labelsCfg['authorAction']) || REQUEST_STATUS_LABEL_REQUESTER_ACTION;
      const approverActionLabel = toStringTrim(labelsCfg['approverAction']) || REQUEST_STATUS_LABEL_REVIEW_PENDING;

      const parentOwnerActionLabel =
        toStringTrim(labelsCfg['parentOwnerAction']) || REQUEST_STATUS_LABEL_PARENT_OWNER_ACTION;

      const authorActionKey = normalizeKey(authorActionLabel);
      const approverActionKey = normalizeKey(approverActionLabel);
      const isProgressStateLabel = (k: string): boolean => k === authorActionKey || k === approverActionKey;

      const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';

      const lockedKeys = resolveLockedWorkflowLabelKeys(context);
      const changedKey = normalizeKey(changedLabel);

      const parentOwnerActionKey = normalizeKey(parentOwnerActionLabel);
      if (parentOwnerActionKey) lockedKeys.add(parentOwnerActionKey);

      const effectiveRequestType = template ? resolveEffectiveRequestType(template, parsedFormData) : '';
      const approverRouting = effectiveRequestType
        ? resolveApproverRoutingForRequestType(
            context,
            effectiveRequestType,
            eff.approverUsernames,
            eff.approverPoolUsernames
          )
        : {
            approvalUsernames: uniqLogins([...(eff.approverUsernames || []), ...(eff.approverPoolUsernames || [])]),
            autoAssigneePoolUsernames: uniqLogins(eff.approverPoolUsernames || []),
          };

      const senderIsConfiguredApprover = isConfiguredApprover(sender?.login, approverRouting.approvalUsernames);

      const managedWorkflowKeys = new Set<string>(Array.from(lockedKeys));
      for (const label of [
        authorActionLabel,
        approverActionLabel,
        parentOwnerActionLabel,
        approvedLabel,
        REQUEST_STATUS_LABEL_REJECTED,
      ]) {
        const key = normalizeKey(label);
        if (key) managedWorkflowKeys.add(key);
      }

      // Configured approvers may manage workflow labels manually
      // Keep routing-label lock logic above intact
      if (senderIsConfiguredApprover && changedKey && managedWorkflowKeys.has(changedKey)) {
        return;
      }

      if (changedKey && lockedKeys.has(changedKey) && !isProgressStateLabel(changedKey)) {
        // Let the existing "Approved label" guard handle manual approval attempts.
        const isManualApprovedAdd = action === 'labeled' && labelsMatching([changedLabel], approvedLabel).length > 0;

        if (!isManualApprovedAdd) {
          if (action === 'labeled') {
            await removeExactLabelsFromIssue(context, params, [changedLabel]);
          } else if (action === 'unlabeled') {
            try {
              await context.octokit.issues.addLabels({ ...params, labels: [changedLabel] });
            } catch {
              // ignore label add errors
            }
          }

          await postOnce(
            context,
            params,
            `Label "${changedLabel}" was reverted. Workflow labels from config are managed by the bot and cannot be changed manually.`,
            { minimizeTag: 'nsreq:workflow-label-lock' }
          );

          return;
        }
      }

      // 3) Manual "Approved" label => rollback
      if (action === 'labeled' && labelsMatching([changedLabel], approvedLabel).length) {
        const approvedMatches = labelsMatching(labels, approvedLabel);
        await removeExactLabelsFromIssue(context, params, approvedMatches);

        // Best effort: keep existing progress label
        const hasAuthor = labelsMatching(labels, authorActionLabel).length > 0;
        const hasReview = labelsMatching(labels, approverActionLabel).length > 0;
        await setStateLabel(context, params, issue, hasAuthor ? 'author' : hasReview ? 'review' : 'review');

        await postOnce(
          context,
          params,
          'Approved label change reverted. Please comment "Approved" to approve a request.',
          { minimizeTag: 'nsreq:label-guard' }
        );
        return;
      }

      // 4) Manual "Rejected" on open issues => rollback
      if (
        action === 'labeled' &&
        labelsMatching([changedLabel], REQUEST_STATUS_LABEL_REJECTED).length &&
        toStringTrim(issue.state).toLowerCase() !== 'closed'
      ) {
        const rejectedMatches = labelsMatching(labels, REQUEST_STATUS_LABEL_REJECTED);
        await removeExactLabelsFromIssue(context, params, rejectedMatches);

        await postOnce(
          context,
          params,
          'Rejected label change reverted. Rejected is set automatically when a request is closed without approval.',
          { minimizeTag: 'nsreq:label-guard' }
        );
        return;
      }

      // 5) Closed issues: enforce terminal state (Approved vs Rejected) + cleanup
      if (toStringTrim(issue.state).toLowerCase() === 'closed') {
        let latest = labels;
        try {
          latest = await fetchIssueLabels(context, params);
        } catch {
          // ignore
        }

        const hasApproved = labelsMatching(latest, approvedLabel).length > 0;

        if (hasApproved) {
          await removeRejectedStatusLabel(context, params, latest);
          await removeProgressStatusLabels(context, params, latest);
          return;
        }

        const hasRejected = labelsMatching(latest, REQUEST_STATUS_LABEL_REJECTED).length > 0;
        if (!hasRejected) {
          try {
            await context.octokit.issues.addLabels({
              ...params,
              labels: [REQUEST_STATUS_LABEL_REJECTED],
            });
          } catch {
            // ignore
          }
        }

        try {
          latest = await fetchIssueLabels(context, params);
        } catch {
          // ignore
        }

        await removeProgressStatusLabels(context, params, latest);

        // mutual exclusivity
        const approvedMatches = labelsMatching(latest, approvedLabel);
        if (approvedMatches.length) {
          await removeExactLabelsFromIssue(context, params, approvedMatches);
        }
        return;
      }
    }
  );

  app.on(
    ['issue_comment.created', 'issue_comment.edited'],
    async (context: BotContext<'issue_comment.created' | 'issue_comment.edited'>): Promise<void> => {
      await getStaticConfig(context);

      const issue = context.payload.issue as unknown as IssueLike;
      const comment = context.payload.comment as unknown as CommentLike;
      const sender = context.payload.sender as unknown as SenderLike;

      const commenter = String(comment?.user?.login || '');

      if (DBG) {
        log(
          context,
          'debug',
          {
            event: context.name,
            action: (context.payload as unknown as Record<string, unknown>)?.action,
            issue: issue?.number,
            commenter,
          },
          'requestHandler:issue-comment-event'
        );
      }

      if (isBotSender(sender)) return;

      const { owner, repo, issue_number: issueNumber } = context.issue() as IssueParams;
      const params: IssueParams = { owner, repo, issue_number: issueNumber };
      const repoInfo: RepoInfo = { owner, repo };

      const stripped = stripQuoteAndCode(comment.body || '');
      const isApproval = isApprovalCommentForContext(context, stripped);

      if (!process.env.JEST_WORKER_ID && !hasIssueFormInputs(issue)) {
        const isPullRequestConversation = isPlainObject((issue as Record<string, unknown>)['pull_request']);

        if (isPullRequestConversation && isApproval) {
          const pr = await readFreshPullRequest(context, repoInfo, issueNumber);
          if (pr && parseLinkedIssueNumberFromPr(pr, repoInfo) === null) {
            await handleDirectPrApprovalComment(context, repoInfo, pr, commenter);
          }
        }

        return;
      }

      let template: TemplateLike;
      try {
        template = await loadTemplateWithLabelRefresh(context, params, issue);
      } catch (e: unknown) {
        log(
          context,
          'error',
          { err: e instanceof Error ? e.message : String(e), owner, repo, issue: issue?.number },
          'Error loading template in issue_comment handler'
        );
        return;
      }

      const parsedFormData = template ? parseForm(readIssueBodyForProcessing(issue.body), template) : {};
      if (!isRequestIssue(context, template, parsedFormData)) {
        if (DBG) {
          log(
            context,
            'debug',
            { issue: issue.number, parsedKeys: Object.keys(parsedFormData || {}) },
            'requestHandler:issue-comment-event skipped (not a request issue)'
          );
        }
        return;
      }

      if (isApproval) {
        const handled = await handleParentOwnerApprovalIfNeeded(
          context,
          params,
          issue,
          template,
          parsedFormData,
          commenter
        );
        if (handled) return;

        const contactHandled = await handleSystemContactOwnerApprovalIfNeeded(
          context,
          params,
          issue,
          template,
          parsedFormData,
          commenter
        );
        if (contactHandled) return;

        await handleApprovalComment(context, params, issue, template, parsedFormData, commenter);
        return;
      }

      if (comment.user.login === issue.user?.login) {
        const saysUpdated = isAuthorUpdateComment(comment.body);
        if (!saysUpdated) return;
        await handleAuthorUpdateComment(app, context, params, issue, template, parsedFormData);
      }
    }
  );

  const runAutoMergeEvaluation = async (
    context: BotContext<RequestEvents>,
    repoInfo: RepoInfo,
    normalizedHeadSha: string
  ): Promise<void> => {
    await runAutoMergeEvaluationApplication(context, repoInfo, normalizedHeadSha, buildAutoMergeTriggerCallbacks());
  };

  const tryAutoMerge = async (
    context: BotContext<RequestEvents>,
    repoInfo: RepoInfo,
    headSha: string
  ): Promise<void> => {
    await tryAutoMergeApplication(context, repoInfo, headSha, runAutoMergeEvaluation, buildAutoMergeTriggerCallbacks());
  };

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
  > => ({
    readCheckRunFromPayload,
    readCheckSuiteFromPayload,
    readRepoInfoFromPayload,
    readCheckRunPrNumbers,
    resolveCheckSuitePrNumbers,
    readCheckSuiteId,
    listAllCheckRunsForSuite,
    readCheckRunId,
    readFirstRegistryValidationArtifactsForSuiteRuns,
    readPullRequestHtmlUrl: async (
      context: BotContext<RequestEvents>,
      repoInfo: RepoInfo,
      prNumber: number
    ): Promise<string> => {
      const pr = await context.octokit.pulls.get({
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        pull_number: prNumber,
      });
      return toStringTrim((pr.data as { html_url?: string })?.html_url);
    },
    collapseBotCommentsByPrefix,
    postCheckSuiteRegistryValidationComments,
    maybeHandleDefaultBranchCheckSuiteSuccess,
    tryAutoMerge,
    handleBlockingRegistryHeadConclusion,
    isBlockingCheckConclusion,
    readDefaultBranchFromPayload,
    getStaticConfig: async (context: BotContext<RequestEvents>): Promise<unknown> => await getStaticConfig(context),
    log,
    isDebugEnabled: DBG,
  });

  app.on('push', async (context: BotContext<'push'>): Promise<void> => {
    const payload = context.payload as unknown;
    const repoInfo = readRepoInfoFromPayload(payload);
    const ref = isPlainObject(payload) ? toStringTrim(payload['ref']) : '';
    const baseBranch = readDefaultBranchFromPush(payload);
    const changedFiles = readPushChangedFiles(payload);
    const approvalConfigChangedFiles = changedFiles.filter(isApprovalConfigChangePath);
    const defaultBranchPush = isDefaultBranchPush(payload);

    log(
      context,
      'info',
      {
        event: toStringTrim((context as unknown as { name?: string }).name),
        ref,
        defaultBranch: baseBranch,
        isDefaultBranchPush: defaultBranchPush,
        owner: repoInfo?.owner,
        repo: repoInfo?.repo,
        changedFilesCount: changedFiles.length,
        approvalConfigChangedFiles,
      },
      'default-branch-push:received'
    );

    if (!defaultBranchPush) return;

    if (!repoInfo) {
      log(
        context,
        'warn',
        {
          ref,
          defaultBranch: baseBranch,
        },
        'default-branch-push:missing-repo-info'
      );
      return;
    }

    await getStaticConfig(context, { forceReload: true });

    const directPrReevaluationReason = approvalConfigChangedFiles.length
      ? 'default-branch-push:approval-config-change'
      : 'default-branch-push:direct-pr-reevaluation';

    const directResult = await reevaluateOpenDirectPullRequestsAfterDefaultBranchPush(
      context,
      repoInfo,
      baseBranch,
      directPrReevaluationReason
    );

    if (!directResult.updated && !directResult.processed && !directResult.blockedByActive) {
      await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry(context, repoInfo, baseBranch);
    }
  });

  app.on(
    ['check_suite.completed', 'check_run.completed'],
    async (context: BotContext<'check_suite.completed' | 'check_run.completed'>): Promise<void> => {
      const payload = context.payload as unknown;
      const eventName = toStringTrim((context as unknown as { name?: string }).name);
      await handleCheckCompletedEventApplication(context, payload, eventName, buildCheckCompletedHandlerCallbacks());
    }
  );

  app.on('status', async (context: BotContext<'status'>): Promise<void> => {
    const payload = context.payload as unknown;
    const state = isPlainObject(payload) ? toStringTrim(payload['state']) : '';
    if (state !== 'success') return;

    const repoObj = isPlainObject(payload) ? payload['repository'] : undefined;
    const repoName = isPlainObject(repoObj) ? toStringTrim(repoObj['name']) : '';
    const ownerObj = isPlainObject(repoObj) ? repoObj['owner'] : undefined;
    const ownerLogin = isPlainObject(ownerObj) ? toStringTrim(ownerObj['login']) : '';

    const sha = isPlainObject(payload) ? toStringTrim(payload['sha']) : '';
    if (!ownerLogin || !repoName || !sha) return;

    await tryAutoMerge(context, { owner: ownerLogin, repo: repoName }, sha);
  });
}
