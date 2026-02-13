import type { Probot, Context } from 'probot';
import { loadStaticConfig } from '../../config.js';

const DBG = process.env.DEBUG_NS === '1';

const RELEVANT_PATHS = [
  '.github/registry-bot/config.yaml',
  '.github/registry-bot/config.yml',
  '.github/registry-bot/config.js',
] as const;

type PushCommit = {
  added?: string[];
  modified?: string[];
  removed?: string[];
};

type PushRepository = {
  default_branch?: string;
};

type PushPayload = {
  ref?: string; // e.g. refs/heads/main
  repository?: PushRepository;
  commits?: PushCommit[];
};

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function asStringArray(v: unknown): string[] {
  if (!Array.isArray(v)) return [];
  return v.map((x) => String(x ?? '')).filter(Boolean);
}

function getPushPayload(payload: unknown): PushPayload {
  if (!isPlainObject(payload)) return {};
  return payload as PushPayload;
}

const configHandler = (app: Probot): void => {
  async function validateStaticConfigOnEvent(
    context: Context<'push'>,
    reason: string,
    forceReload: boolean = false
  ): Promise<void> {
    const { owner, repo } = context.repo();

    if (DBG && context.log?.debug) {
      context.log.debug({ owner, repo, reason, forceReload }, 'static-config:handler:validate-on-event');
    }

    try {
      await loadStaticConfig(context, { validate: true, updateIssue: true, forceReload });
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      context.log?.warn?.(
        { err: msg, owner, repo, reason, forceReload },
        'failed to validate static config via config handler'
      );
    }
  }

  app.on('push', (context: Context<'push'>) => {
    const payload = getPushPayload(context.payload);

    const ref = typeof payload.ref === 'string' ? payload.ref : '';
    const defaultBranch =
      typeof payload.repository?.default_branch === 'string' ? payload.repository.default_branch : '';

    // Validate static config only on the default branch
    if (!defaultBranch || ref !== `refs/heads/${defaultBranch}`) {
      if (DBG && context.log?.debug) {
        context.log.debug({ ref, defaultBranch }, 'static-config:handler:skip-non-default-branch');
      }
      return;
    }

    const commits = Array.isArray(payload.commits) ? payload.commits : [];
    const changedFiles = new Set<string>();

    for (const c of commits) {
      for (const f of asStringArray(c?.added)) changedFiles.add(f);
      for (const f of asStringArray(c?.modified)) changedFiles.add(f);
      for (const f of asStringArray(c?.removed)) changedFiles.add(f);
    }

    const touchedConfig = RELEVANT_PATHS.some((p) => changedFiles.has(p));
    const reason = touchedConfig ? 'push-config-change' : 'push-default-branch';

    if (DBG && context.log?.debug) {
      context.log.debug(
        {
          ref,
          defaultBranch,
          changedFiles: Array.from(changedFiles),
          relevantPaths: RELEVANT_PATHS,
          touchedConfig,
          reason,
        },
        'static-config:handler:push-event'
      );
    }

    // If config files changed, bypass cache to validate the new version immediately
    return validateStaticConfigOnEvent(context, reason, touchedConfig);
  });
};

export default configHandler;
