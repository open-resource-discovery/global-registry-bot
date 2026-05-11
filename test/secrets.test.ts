import { loadSecrets } from '../src/utils/secrets.js';

function b64(s: string): string {
  return Buffer.from(s, 'utf8').toString('base64');
}

test('returns defaults when env is empty', () => {
  const s = loadSecrets({});

  expect(s).toEqual({
    APP_ID: undefined,
    WEBHOOK_SECRET: undefined,
    PRIVATE_KEY: undefined,
    DEBUG_NS: '1',
    HOOK_SECRETS: {},
  });
});

test('reads core secrets and generic HOOK_SECRET_* values', () => {
  const s = loadSecrets({
    APP_ID: 'app',
    WEBHOOK_SECRET: 'wh',
    DEBUG_NS: '9',
    HOOK_SECRET_STC_URL: ' https://stc.example ',
    HOOK_SECRET_BASIC_AUTH: ' Basic abc123 ',
    HOOK_SECRET_PPMS_URL: ' https://ppms.example ',
  });

  expect(s.APP_ID).toBe('app');
  expect(s.WEBHOOK_SECRET).toBe('wh');
  expect(s.DEBUG_NS).toBe('9');

  expect(s.HOOK_SECRETS).toEqual({
    STC_URL: 'https://stc.example',
    BASIC_AUTH: 'Basic abc123',
    PPMS_URL: 'https://ppms.example',
  });
});

test('strips HOOK_SECRET_ prefix and does not expose prefixed names', () => {
  const s = loadSecrets({
    HOOK_SECRET_STC_URL: 'https://stc.example',
    HOOK_SECRET_BASIC_AUTH: 'Basic secret',
  });

  expect(s.HOOK_SECRETS.STC_URL).toBe('https://stc.example');
  expect(s.HOOK_SECRETS.BASIC_AUTH).toBe('Basic secret');

  expect(s.HOOK_SECRETS.HOOK_SECRET_STC_URL).toBeUndefined();
  expect(s.HOOK_SECRETS.HOOK_SECRET_BASIC_AUTH).toBeUndefined();
});

test('ignores old legacy hook secret env variables', () => {
  const s = loadSecrets({
    CLD_API_BASE_URL: 'https://cld.example',
    CLD_API_KEY: 'cld',
    STC_API_BASE_URL: 'https://stc.example',
    STC_API_KEY: 'stc',
    PPMS_API_BASE_URL: 'https://ppms.example',
    PPMS_API_KEY: 'ppms',
  });

  expect(s.HOOK_SECRETS).toEqual({});
  expect(s.HOOK_SECRETS.CLD_API_BASE_URL).toBeUndefined();
  expect(s.HOOK_SECRETS.STC_API_BASE_URL).toBeUndefined();
  expect(s.HOOK_SECRETS.PPMS_API_KEY).toBeUndefined();
});

test('ignores non-prefixed hook-looking variables', () => {
  const s = loadSecrets({
    STC_URL: 'https://stc.example',
    BASIC_AUTH: 'Basic secret',
    PPMS_URL: 'https://ppms.example',
  });

  expect(s.HOOK_SECRETS).toEqual({});
});

test('trims hook secret values and skips empty names or empty values', () => {
  const s = loadSecrets({
    HOOK_SECRET_: 'should-be-ignored',
    HOOK_SECRET_EMPTY: '',
    HOOK_SECRET_BLANK: '   ',
    HOOK_SECRET_VALID: ' valid-value ',
  });

  expect(s.HOOK_SECRETS).toEqual({
    VALID: 'valid-value',
  });
});

test('skips nullish hook secret values defensively', () => {
  const s = loadSecrets({
    HOOK_SECRET_VALID: 'ok',
    // NodeJS.ProcessEnv normally only has string | undefined.
    // This keeps the defensive null branch covered.
    HOOK_SECRET_NULLISH: null,
  } as unknown as NodeJS.ProcessEnv);

  expect(s.HOOK_SECRETS).toEqual({
    VALID: 'ok',
  });
});

test('PRIVATE_KEY wins if it looks like PEM (contains BEGIN) and is trimmed', () => {
  const pem = ' \n-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n ';
  const s = loadSecrets({
    PRIVATE_KEY: pem,
    PRIVATE_KEY_B64: b64('should-not-be-used'),
  });

  expect(s.PRIVATE_KEY).toBe('-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----');
});

test('falls back to PRIVATE_KEY_B64 if PRIVATE_KEY is not a PEM', () => {
  const decoded = '-----BEGIN PRIVATE KEY-----\nxyz\n-----END PRIVATE KEY-----';
  const s = loadSecrets({
    PRIVATE_KEY: 'not-a-pem',
    PRIVATE_KEY_B64: b64(decoded),
  });

  expect(s.PRIVATE_KEY).toBe(decoded);
});

test('PRIVATE_KEY stays undefined if neither PEM nor B64 are provided', () => {
  const s = loadSecrets({ PRIVATE_KEY: 'not-a-pem' });

  expect(s.PRIVATE_KEY).toBeUndefined();
});

test('DEBUG_NS empty string stays empty (no fallback)', () => {
  const s = loadSecrets({ DEBUG_NS: '' });

  expect(s.DEBUG_NS).toBe('');
});

test('returned objects are frozen', () => {
  const s = loadSecrets({
    APP_ID: 'x',
    HOOK_SECRET_BASIC_AUTH: 'Basic secret',
  });

  expect(Object.isFrozen(s)).toBe(true);
  expect(Object.isFrozen(s.HOOK_SECRETS)).toBe(true);

  expect(() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (s as any).APP_ID = 'y';
  }).toThrow();

  expect(() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (s.HOOK_SECRETS as any).BASIC_AUTH = 'changed';
  }).toThrow();

  expect(() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (s.HOOK_SECRETS as any).NEW_SECRET = 'new';
  }).toThrow();
});
