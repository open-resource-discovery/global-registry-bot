/* eslint-disable require-await */
import { jest } from '@jest/globals';

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type, @typescript-eslint/no-explicit-any
async function importConfigHandler(loadStaticConfigMock: any) {
  jest.resetModules();

  await jest.unstable_mockModule('../src/config.js', () => ({
    loadStaticConfig: loadStaticConfigMock,
  }));

  const mod = await import('../src/handlers/config/index.js');
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return mod.default as any;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkApp() {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handlers: any[] = [];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const app: any = {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    on: (event: string, fn: any) => {
      if (event === 'push') handlers.push(fn);
    },
  };
  return { app, handlers };
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type, @typescript-eslint/no-explicit-any
function mkContext(payload: any) {
  return {
    payload,
    repo: () => ({ owner: 'o', repo: 'r' }),
    log: { debug: jest.fn(), warn: jest.fn() },
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any;
}

test('skips push events not on default branch', async () => {
  const loadStaticConfig = jest.fn(async () => ({}));
  const handler = await importConfigHandler(loadStaticConfig);

  const { app, handlers } = mkApp();
  handler(app);

  const push = handlers[0];
  await push(
    mkContext({
      ref: 'refs/heads/feature',
      repository: { default_branch: 'main' },
      commits: [],
    })
  );

  expect(loadStaticConfig).not.toHaveBeenCalled();
});

test('validates on default branch with forceReload=false when config not touched', async () => {
  const loadStaticConfig = jest.fn(async () => ({}));
  const handler = await importConfigHandler(loadStaticConfig);

  const { app, handlers } = mkApp();
  handler(app);

  const push = handlers[0];
  const ctx = mkContext({
    ref: 'refs/heads/main',
    repository: { default_branch: 'main' },
    commits: [{ added: ['a.txt'], modified: [], removed: [] }],
  });

  await push(ctx);

  expect(loadStaticConfig).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ validate: true, updateIssue: true, forceReload: false })
  );
});

test('validates on default branch with forceReload=true when config touched', async () => {
  const loadStaticConfig = jest.fn(async () => ({}));
  const handler = await importConfigHandler(loadStaticConfig);

  const { app, handlers } = mkApp();
  handler(app);

  const push = handlers[0];
  const ctx = mkContext({
    ref: 'refs/heads/main',
    repository: { default_branch: 'main' },
    commits: [{ modified: ['.github/registry-bot/config.yaml'] }],
  });

  await push(ctx);

  expect(loadStaticConfig).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ validate: true, updateIssue: true, forceReload: true })
  );
});

test('swallows loadStaticConfig errors and logs warn', async () => {
  const loadStaticConfig = jest.fn(async () => {
    throw new Error('fail');
  });
  const handler = await importConfigHandler(loadStaticConfig);

  const { app, handlers } = mkApp();
  handler(app);

  const push = handlers[0];
  const ctx = mkContext({
    ref: 'refs/heads/main',
    repository: { default_branch: 'main' },
    commits: [{ modified: ['a.txt'] }],
  });

  await push(ctx);

  expect(ctx.log.warn).toHaveBeenCalled();
});
