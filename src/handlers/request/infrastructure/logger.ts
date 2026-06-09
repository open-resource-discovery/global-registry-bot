export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export type LoggerFn = (this: unknown, obj: unknown, msg?: string) => void;

export type LoggerLike = Partial<Record<LogLevel, LoggerFn>>;

export function log(context: { log?: LoggerLike } | undefined, level: LogLevel, obj: unknown, msg: string): void {
  const logger = context?.log;
  const fn = logger?.[level];

  if (typeof fn === 'function') {
    fn.call(logger, obj, msg);
  }
}
