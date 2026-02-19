export type HookSecrets = Readonly<{
  CLD_API_BASE_URL?: string;
  CLD_API_KEY?: string;

  STC_API_BASE_URL?: string;
  STC_API_KEY?: string;

  PPMS_API_BASE_URL?: string;
  PPMS_API_KEY?: string;
}>;

export type CoreSecrets = Readonly<{
  APP_ID?: string;
  WEBHOOK_SECRET?: string;
  PRIVATE_KEY?: string;
  DEBUG_NS: string;
  HOOK_SECRETS: HookSecrets;
}>;

export const coreSecrets: CoreSecrets = Object.freeze(loadSecrets());

export function loadSecrets(env: NodeJS.ProcessEnv = process.env): CoreSecrets {
  const get = (key: string, fallback?: string): string | undefined => {
    const value = env[key];
    return value !== undefined && value !== null ? String(value) : fallback;
  };

  const privateKeyPem = normalizePem(get('PRIVATE_KEY')) ?? decodeB64(get('PRIVATE_KEY_B64'));

  const hookSecrets: HookSecrets = Object.freeze({
    CLD_API_BASE_URL: get('CLD_API_BASE_URL'),
    CLD_API_KEY: get('CLD_API_KEY'),

    STC_API_BASE_URL: get('STC_API_BASE_URL'),
    STC_API_KEY: get('STC_API_KEY'),

    PPMS_API_BASE_URL: get('PPMS_API_BASE_URL'),
    PPMS_API_KEY: get('PPMS_API_KEY'),
  });

  return Object.freeze({
    APP_ID: get('APP_ID'),
    WEBHOOK_SECRET: get('WEBHOOK_SECRET'),
    PRIVATE_KEY: privateKeyPem,
    DEBUG_NS: get('DEBUG_NS', '1') ?? '1',
    HOOK_SECRETS: hookSecrets,
  });
}

function decodeB64(value?: string): string | undefined {
  if (!value) return undefined;
  return Buffer.from(value, 'base64').toString('utf8');
}

function normalizePem(value?: string): string | undefined {
  if (!value) return undefined;
  const v = value.trim();
  return v.includes('BEGIN') ? v : undefined;
}
