/* eslint-disable require-await */
import { vi } from 'vitest';

// biome-ignore lint/suspicious/noExplicitAny: Existing exception retained during tooling migration.
async function importConfigHandler(loadStaticConfigMock: any) {
  vi.resetModules();

  await vi.doMock('../src/config.js', () => ({
    loadStaticConfig: loadStaticConfigMock,
  }));

  const mod = await import('../src/handlers/config/index.js');
  // biome-ignore lint/suspicious/noExplicitAny: Existing exception retained during tooling migration.
  return mod.default as any;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkApp() {
  // biome-ignore lint/suspicious/noExplicitAny: Existing exception retained during tooling migration.
  const handlers: any[] = [];
  // biome-ignore lint/suspicious/noExplicitAny: Existing exception retained during tooling migration.
  const app: any = {
    // biome-ignore lint/suspicious/noExplicitAny: Existing exception retained during tooling migration.
    on: (event: string, fn: any) => {
      if (event === 'push') handlers.push(fn);
    },
  };
  return { app, handlers };
}

// biome-ignore lint/suspicious/noExplicitAny: Existing exception retained during tooling migration.
function mkContext(payload: any) {
  return {
    payload,
    repo: () => ({ owner: 'o', repo: 'r' }),
    log: { debug: vi.fn(), warn: vi.fn() },
    // biome-ignore lint/suspicious/noExplicitAny: Existing exception retained during tooling migration.
  } as any;
}

test('skips push events not on default branch', async () => {
  const loadStaticConfig = vi.fn(async () => ({}));
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
  const loadStaticConfig = vi.fn(async () => ({}));
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
  const loadStaticConfig = vi.fn(async () => ({}));
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
  const loadStaticConfig = vi.fn(async () => {
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
