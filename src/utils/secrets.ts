export type HookSecrets = Readonly<Record<string, string | undefined>>;

export type CoreSecrets = Readonly<{
  APP_ID?: string;
  WEBHOOK_SECRET?: string;
  PRIVATE_KEY?: string;
  DEBUG_NS: string;
  HOOK_SECRETS: HookSecrets;
}>;

export const coreSecrets: CoreSecrets = Object.freeze(loadSecrets());

function collectHookSecretsFromEnv(env: NodeJS.ProcessEnv): Record<string, string> {
  const out: Record<string, string> = {};

  for (const [key, value] of Object.entries(env)) {
    if (!key.startsWith('HOOK_SECRET_')) continue;
    if (value === undefined || value === null) continue;

    const secretName = key.slice('HOOK_SECRET_'.length).trim();
    const secretValue = String(value).trim();

    if (!secretName || !secretValue) continue;

    out[secretName] = secretValue;
  }

  return out;
}

export function loadSecrets(env: NodeJS.ProcessEnv = process.env): CoreSecrets {
  const get = (key: string, fallback?: string): string | undefined => {
    const value = env[key];
    return value !== undefined && value !== null ? String(value) : fallback;
  };

  const privateKeyPem = normalizePem(get('PRIVATE_KEY')) ?? decodeB64(get('PRIVATE_KEY_B64'));

  const hookSecrets: HookSecrets = Object.freeze(collectHookSecretsFromEnv(env));

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
