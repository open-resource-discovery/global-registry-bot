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
    HOOK_SECRETS: {
      CLD_API_BASE_URL: undefined,
      CLD_API_KEY: undefined,
      STC_API_BASE_URL: undefined,
      STC_API_KEY: undefined,
      PPMS_API_BASE_URL: undefined,
      PPMS_API_KEY: undefined,
    },
  });
});

test('reads core secrets and hook secrets', () => {
  const s = loadSecrets({
    APP_ID: 'app',
    WEBHOOK_SECRET: 'wh',
    DEBUG_NS: '9',
    CLD_API_BASE_URL: 'https://cld.example',
    CLD_API_KEY: 'cld',
    STC_API_BASE_URL: 'https://stc.example',
    STC_API_KEY: 'stc',
    PPMS_API_BASE_URL: 'https://ppms.example',
    PPMS_API_KEY: 'ppms',
  });

  expect(s.APP_ID).toBe('app');
  expect(s.WEBHOOK_SECRET).toBe('wh');
  expect(s.DEBUG_NS).toBe('9');
  expect(s.HOOK_SECRETS.CLD_API_BASE_URL).toBe('https://cld.example');
  expect(s.HOOK_SECRETS.PPMS_API_KEY).toBe('ppms');
});

test('PRIVATE_KEY wins if it looks like PEM (contains BEGIN) and is trimmed', () => {
  const pem = ' \n-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n ';
  const s = loadSecrets({
    PRIVATE_KEY: pem,
    PRIVATE_KEY_B64: b64('should-not-be-used'),
  });

  expect(s.PRIVATE_KEY).toContain('BEGIN PRIVATE KEY');
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
  const s = loadSecrets({ APP_ID: 'x' });

  expect(Object.isFrozen(s)).toBe(true);
  expect(Object.isFrozen(s.HOOK_SECRETS)).toBe(true);

  expect(() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (s as any).APP_ID = 'y';
  }).toThrow();

  expect(() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (s.HOOK_SECRETS as any).CLD_API_KEY = 'y';
  }).toThrow();
});
