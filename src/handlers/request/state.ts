import { getStateLabelsFromConfig, getApproversFromConfig } from './constants.js';

type LabelLike = string | { name?: string | null };

type AssigneeLike = { login?: string | null };

type IssueLike = {
  labels?: LabelLike[] | null;
  assignees?: AssigneeLike[] | null;
};

type IssueParams = {
  owner: string;
  repo: string;
  issue_number: number;
};

type OctokitLike = {
  issues: {
    removeLabel: (args: IssueParams & { name: string }) => Promise<unknown>;
    addLabels: (args: IssueParams & { labels: string[] }) => Promise<unknown>;
    addAssignees: (args: IssueParams & { assignees: string[] }) => Promise<unknown>;
  };
};

type LoggerLike = {
  warn?: (obj: unknown, msg?: string) => void;
};

type ContextLike = {
  octokit: OctokitLike;
  log?: LoggerLike;
  resourceBotConfig?: Record<string, unknown>;
};

type WorkflowStateConfig = {
  authorLabel: string | null;
  reviewLabel: string | null;
  globalLabels: string[];
  approvers: string[];
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function getHttpStatus(err: unknown): number | undefined {
  if (!isPlainObject(err)) return undefined;
  const s = err['status'];
  return typeof s === 'number' ? s : undefined;
}

function getErrorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (isPlainObject(err) && typeof err['message'] === 'string') return String(err['message']);
  return String(err);
}

function isSecondaryRateLimit(err: unknown): boolean {
  const status = getHttpStatus(err);
  if (status !== 403 && status !== 429) return false;
  return /secondary rate limit/i.test(getErrorMessage(err));
}

function toLabelString(l: LabelLike): string {
  if (typeof l === 'string') return l;
  return String(l?.name ?? '');
}

function getWorkflowStateConfig(context: ContextLike): WorkflowStateConfig {
  const cfg = context?.resourceBotConfig;

  const wf = isPlainObject(cfg) ? cfg['workflow'] : undefined;
  const labels = isPlainObject(wf) ? wf['labels'] : undefined;

  const { author, review } = getStateLabelsFromConfig(cfg);

  const globalRaw = isPlainObject(labels) ? labels['global'] : undefined;
  const globalLabels = Array.isArray(globalRaw)
    ? globalRaw.map((x) => String(x ?? '').trim()).filter(Boolean)
    : globalRaw
      ? [String(globalRaw).trim()].filter(Boolean)
      : [];

  const approvers = getApproversFromConfig(cfg);

  return {
    authorLabel: author || null,
    reviewLabel: review || null,
    globalLabels,
    approvers,
  };
}

export async function setStateLabel(
  context: ContextLike,
  params: IssueParams,
  issue: IssueLike,
  state: 'author' | 'review'
): Promise<void> {
  const { authorLabel, reviewLabel, globalLabels } = getWorkflowStateConfig(context);

  const add = state === 'author' ? authorLabel : reviewLabel;
  const remove = state === 'author' ? reviewLabel : authorLabel;

  if (!add && (!globalLabels || !globalLabels.length)) return;

  const currentLabels = (issue?.labels || []).map((l) => toLabelString(l));
  const labelsToAdd = [add, ...(globalLabels || [])].filter((x): x is string => Boolean(x));

  const newLabels = labelsToAdd.filter((l) => !currentLabels.includes(l));
  const shouldRemove = Boolean(remove && currentLabels.includes(remove));

  if (!shouldRemove && newLabels.length === 0) return;

  if (shouldRemove && remove) {
    try {
      // removeLabel expects { owner, repo, issue_number, name }
      await context.octokit.issues.removeLabel({ ...params, name: remove });
    } catch (err: unknown) {
      if (isSecondaryRateLimit(err)) {
        context.log?.warn?.(
          { err: getErrorMessage(err), label: remove },
          'setStateLabel: removeLabel hit secondary rate limit'
        );
        return;
      }
    }
  }

  if (newLabels.length === 0) return;

  try {
    await context.octokit.issues.addLabels({ ...params, labels: newLabels });
  } catch (err: unknown) {
    if (isSecondaryRateLimit(err)) {
      context.log?.warn?.(
        { err: getErrorMessage(err), labels: newLabels },
        'setStateLabel: addLabels hit secondary rate limit'
      );
      return;
    }
  }
}

export async function ensureAssigneesOnce(
  context: ContextLike,
  params: IssueParams,
  issue: IssueLike,
  logins: string[] = []
): Promise<void> {
  const { approvers } = getWorkflowStateConfig(context);

  const desired = Array.isArray(logins) && logins.length ? logins : approvers;
  if (!desired.length) return;

  const current = (issue.assignees || []).map((a) => String(a.login ?? '')).filter(Boolean);
  const missing = desired.filter((a) => !current.includes(a));
  if (!missing.length) return;

  try {
    await context.octokit.issues.addAssignees({ ...params, assignees: missing });
  } catch {
    // keep behavior: ignore
  }
}
