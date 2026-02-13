import { jest } from '@jest/globals';
import * as fsReal from 'node:fs';
import * as path from 'node:path';

const handlersRoot = path.join(process.cwd(), 'src', 'handlers');

type AppEntryFn = (app: unknown, context: unknown) => Promise<void>;

async function importAppEntry(args: { secrets: unknown; fsMock?: unknown }): Promise<AppEntryFn> {
  jest.resetModules();

  await jest.unstable_mockModule('dotenv/config', () => ({}));

  await jest.unstable_mockModule('../src/utils/secrets.js', () => ({
    coreSecrets: args.secrets,
  }));

  if (args.fsMock) {
    await jest.unstable_mockModule('fs', () => ({ default: args.fsMock }));
  }

  const mod = await import('../src/index.js');
  return mod.default as AppEntryFn;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkApp() {
  return {
    log: {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    },
  } as unknown;
}

test('throws when required secrets are missing', async () => {
  jest.resetModules();

  await jest.unstable_mockModule('dotenv/config', () => ({}));
  await jest.unstable_mockModule('../src/utils/secrets.js', () => ({
    coreSecrets: { APP_ID: '', WEBHOOK_SECRET: '', PRIVATE_KEY: '' },
  }));

  await expect(import('../src/index.js')).rejects.toThrow('Missing secrets: APP_ID/WEBHOOK_SECRET/PRIVATE_KEY');
});

test('logs and returns if handlers directory cannot be read', async () => {
  const fsMock = {
    // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
    readdirSync: () => {
      throw new Error('boom');
    },
    // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
    existsSync: () => false,
  };

  const appEntry = await importAppEntry({
    secrets: { APP_ID: '1', WEBHOOK_SECRET: '1', PRIVATE_KEY: '1' },
    fsMock,
  });

  const app = mkApp() as {
    log: { info: jest.Mock; warn: jest.Mock; error: jest.Mock; debug: jest.Mock };
  };
  await appEntry(app, {} as unknown);

  expect(app.log.info).toHaveBeenCalledWith('Request Bot booting...');
  expect(app.log.error).toHaveBeenCalledWith(
    expect.anything(),
    expect.stringContaining('Failed reading handlers directory:')
  );
});

test('scans handlers folder and handles: good handler, missing index, bad default export, import error', async () => {
  const goodName = '__test_good';
  const badName = '__test_bad_export';
  const noIdxName = '__test_no_index';
  const boomName = '__test_boom';

  const goodDir = path.join(handlersRoot, goodName);
  const badDir = path.join(handlersRoot, badName);
  const boomDir = path.join(handlersRoot, boomName);

  const goodIndex = path.join(goodDir, 'index.js');
  const badIndex = path.join(badDir, 'index.js');
  const boomIndex = path.join(boomDir, 'index.js');

  fsReal.mkdirSync(goodDir, { recursive: true });
  fsReal.mkdirSync(badDir, { recursive: true });
  fsReal.mkdirSync(boomDir, { recursive: true });

  fsReal.writeFileSync(goodIndex, `export default async function(app) { app.log.info('handler:good'); }`, 'utf8');
  fsReal.writeFileSync(badIndex, `export default 123;`, 'utf8');
  fsReal.writeFileSync(boomIndex, `throw new Error('boom');`, 'utf8');

  const dirents = [goodName, badName, noIdxName, boomName].map((name) => ({
    name,
    // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
    isDirectory: () => true,
  }));

  const existing = new Set([goodIndex, badIndex, boomIndex]);

  const fsMock = {
    // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
    readdirSync: (_p: string) => dirents,
    // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
    existsSync: (p: string) => existing.has(String(p)),
  };

  try {
    const appEntry = await importAppEntry({
      secrets: { APP_ID: '1', WEBHOOK_SECRET: '1', PRIVATE_KEY: '1' },
      fsMock,
    });

    const app = mkApp() as {
      log: { info: jest.Mock; warn: jest.Mock; error: jest.Mock; debug: jest.Mock };
    };
    await appEntry(app, {} as unknown);

    expect(app.log.info).toHaveBeenCalledWith('Request Bot booting...');
    expect(app.log.info).toHaveBeenCalledWith(expect.stringContaining('Found handler directories:'));

    expect(app.log.info).toHaveBeenCalledWith('handler:good');
    expect(app.log.info).toHaveBeenCalledWith(expect.stringContaining(`Loaded handler: ${goodName}/index.js`));

    expect(app.log.debug).toHaveBeenCalledWith(
      expect.stringContaining(`Skipping handler '${noIdxName}' (no index.js)`)
    );

    expect(app.log.warn).toHaveBeenCalledWith(
      expect.stringContaining(`Skip ${badName}/index.js (no default export function)`)
    );

    expect(app.log.error).toHaveBeenCalledWith(
      expect.anything(),
      expect.stringContaining(`Failed loading handler entry for ${boomName}:`)
    );

    expect(app.log.info).toHaveBeenCalledWith('Request Bot ready (handlers scanned)');
  } finally {
    fsReal.rmSync(goodDir, { recursive: true, force: true });
    fsReal.rmSync(badDir, { recursive: true, force: true });
    fsReal.rmSync(boomDir, { recursive: true, force: true });
  }
});
