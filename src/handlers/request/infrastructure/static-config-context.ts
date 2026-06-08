import {
  DEFAULT_CONFIG,
  loadStaticConfig,
  type NormalizedStaticConfig,
  type RegistryBotHooks,
} from '../../../config.js';
import { log, type LoggerLike } from './logger.js';

export type StaticConfigLoadOptions = {
  forceReload?: boolean;
};

export type ResourceBotContextExt = {
  resourceBotConfig?: NormalizedStaticConfig;
  resourceBotHooks?: RegistryBotHooks | null;
  resourceBotHooksSource?: string | null;
};

type StaticConfigBaseContext = Parameters<typeof loadStaticConfig>[0];

export type StaticConfigContext = StaticConfigBaseContext & ResourceBotContextExt;

type AppLogLike = {
  warn?: (obj: unknown, msg?: string) => void;
  log?: LoggerLike;
};

export function createStaticConfigContextLoader<ContextType extends StaticConfigContext>(
  appLog: AppLogLike | undefined
): (context: ContextType, options?: StaticConfigLoadOptions) => Promise<NormalizedStaticConfig> {
  return async function getStaticConfig(
    context: ContextType,
    options: StaticConfigLoadOptions = {}
  ): Promise<NormalizedStaticConfig> {
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
      (appLog || console).warn?.(
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
}
