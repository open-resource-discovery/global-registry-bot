/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type, require-await */
/* DBG=true variant — must run in its own module instance */
import { jest } from '@jest/globals';

// Set before dynamic import so the module-level `const DBG = process.env.DEBUG_NS === '1'` is true
process.env['DEBUG_NS'] = '1';

const { loadTemplate } = await import('../src/handlers/request/template.js');

// ── helpers ───────────────────────────────────────────────────────────────────

type FileEntry = { kind: 'file'; text: string; encoding?: BufferEncoding } | { kind: 'dir' };

function b64utf8(s: string): string {
  return Buffer.from(s, 'utf8').toString('base64');
}

function mkContext(args: { files: Record<string, FileEntry>; resourceBotConfig?: any }) {
  const getContent = jest.fn(async ({ path }: any) => {
    const entry = args.files[path];
    if (!entry) throw new Error(`missing fixture: ${path}`);
    if (entry.kind === 'dir') return { data: [] };
    const encoding = entry.encoding ?? 'base64';
    const content = encoding === 'base64' ? b64utf8(entry.text) : entry.text;
    return { data: { content, encoding } };
  });

  return {
    context: {
      octokit: { repos: { getContent } },
      resourceBotConfig: args.resourceBotConfig ?? {},
      log: { debug: jest.fn() },
    },
    getContent,
  };
}

// ── DBG L385 + L478: loadTemplate input log and file-fetched log ─────────────

test('DBG: fires tpl:loadTemplate:input and tpl:fetched logs on first fetch (L385, L478)', async () => {
  const owner = 'dbg-owner-l385';
  const repo = 'r';
  const tplPath = '.github/registry-bot/templates/prod.yml';

  const { context } = mkContext({
    files: {
      [tplPath]: { kind: 'file', text: `name: DBG Product\nlabels: [registry-bot:prod]\nbody: []\n` },
    },
    resourceBotConfig: {
      requests: { prod: { issueTemplate: 'templates/prod.yml' } },
    },
  });

  await loadTemplate(context as any, {
    owner,
    repo,
    issueLabels: ['registry-bot:prod'],
  });

  const debugCalls = (context.log.debug as jest.MockedFunction<any>).mock.calls;
  const msgs = debugCalls.map(([, msg]: [unknown, string]) => msg);
  expect(msgs).toContain('tpl:loadTemplate:input');
  expect(msgs).toContain('tpl:fetched');
});

// ── DBG L304: tpl:label-index:built log ──────────────────────────────────────

test('DBG: fires tpl:label-index:built log when label index is first built (L304)', async () => {
  const owner = 'dbg-owner-l304';
  const repo = 'r';
  const tplPath = '.github/registry-bot/templates/svc.yml';

  const { context } = mkContext({
    files: {
      [tplPath]: { kind: 'file', text: `name: DBG Service\nlabels: [registry-bot:svc]\nbody: []\n` },
    },
    resourceBotConfig: {
      requests: { svc: { issueTemplate: 'templates/svc.yml' } },
    },
  });

  await loadTemplate(context as any, {
    owner,
    repo,
    issueLabels: ['registry-bot:svc'],
  });

  const debugCalls = (context.log.debug as jest.MockedFunction<any>).mock.calls;
  const msgs = debugCalls.map(([, msg]: [unknown, string]) => msg);
  expect(msgs).toContain('tpl:label-index:built');
});

// ── DBG L530: tpl:cache-hit log (requestType cache) ──────────────────────────

test('DBG: fires tpl:cache-hit log on second label-routing call for same owner/repo/requestType (L530)', async () => {
  const owner = 'dbg-owner-l530';
  const repo = 'r';
  const tplPath = '.github/registry-bot/templates/ns.yml';

  const { context } = mkContext({
    files: {
      [tplPath]: { kind: 'file', text: `name: DBG NS\nlabels: [registry-bot:ns]\nbody: []\n` },
    },
    resourceBotConfig: {
      requests: { ns: { issueTemplate: 'templates/ns.yml' } },
    },
  });

  // First call: populates caches
  await loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:ns'] });

  // Reset debug mock to detect only second-call logs
  (context.log.debug as jest.MockedFunction<any>).mockClear();

  // Second call: requestType cache hit → fires L530
  await loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:ns'] });

  const debugCalls = (context.log.debug as jest.MockedFunction<any>).mock.calls;
  const msgs = debugCalls.map(([, msg]: [unknown, string]) => msg);
  expect(msgs).toContain('tpl:cache-hit');
});

// ── DBG L203: tpl:label-index:cache-hit log ──────────────────────────────────

test('DBG: fires tpl:label-index:cache-hit log on second routing call for different requestType (L203)', async () => {
  const owner = 'dbg-owner-l203';
  const repo = 'r';
  const tplA = '.github/registry-bot/templates/typeA.yml';
  const tplB = '.github/registry-bot/templates/typeB.yml';

  const { context } = mkContext({
    files: {
      [tplA]: { kind: 'file', text: `name: TypeA\nlabels: [registry-bot:typeA]\nbody: []\n` },
      [tplB]: { kind: 'file', text: `name: TypeB\nlabels: [registry-bot:typeB]\nbody: []\n` },
    },
    resourceBotConfig: {
      requests: {
        typeA: { issueTemplate: 'templates/typeA.yml' },
        typeB: { issueTemplate: 'templates/typeB.yml' },
      },
    },
  });

  // First call: builds and caches label index
  await loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:typeA'] });

  (context.log.debug as jest.MockedFunction<any>).mockClear();

  // Second call with different label → requestType cache miss → calls buildLabelIndexFromTemplates
  // → finds label-index in TEMPLATE_CACHE → fires L203
  await loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:typeB'] });

  const debugCalls = (context.log.debug as jest.MockedFunction<any>).mock.calls;
  const msgs = debugCalls.map(([, msg]: [unknown, string]) => msg);
  expect(msgs).toContain('tpl:label-index:cache-hit');
});

// ── L521: requestType found in cached label-index but has no issueTemplate in current config ──

test('routing: throws when requestType has no issueTemplate but label-index was cached (L521)', async () => {
  const owner = 'l521-dual-owner';
  const repo = 'r';
  const tplA = '.github/registry-bot/templates/l521a.yml';
  const tplB = '.github/registry-bot/templates/l521b.yml';

  // First call: build label index with valid config (both types have issueTemplate)
  const { context: ctx1 } = mkContext({
    files: {
      [tplA]: { kind: 'file', text: `name: L521A\nlabels: [registry-bot:l521a]\nbody: []\n` },
      [tplB]: { kind: 'file', text: `name: L521B\nlabels: [registry-bot:l521b]\nbody: []\n` },
    },
    resourceBotConfig: {
      requests: {
        l521a: { issueTemplate: 'templates/l521a.yml' },
        l521b: { issueTemplate: 'templates/l521b.yml' },
      },
    },
  });

  // Populate label index cache and l521a requestType cache
  await loadTemplate(ctx1 as any, { owner, repo, issueLabels: ['registry-bot:l521a'] });

  // Second call: label index is cached, but l521b config now has no issueTemplate
  const { context: ctx2 } = mkContext({
    files: {},
    resourceBotConfig: {
      requests: {
        l521a: { issueTemplate: 'templates/l521a.yml' },
        l521b: {}, // no issueTemplate → L521 fires after label-index cache hit
      },
    },
  });

  await expect(loadTemplate(ctx2 as any, { owner, repo, issueLabels: ['registry-bot:l521b'] })).rejects.toThrow(
    "requestType 'l521b' not mapped in cfg.requests"
  );
});

// ── DBG L402: tpl:file-cache-hit log ─────────────────────────────────────────

test('DBG: fires tpl:file-cache-hit log on second fetchFile for same path (L402)', async () => {
  const owner = 'dbg-owner-l402';
  const repo = 'r';
  const tplPath = '.github/registry-bot/templates/fcached.yml';

  const { context, getContent } = mkContext({
    files: {
      [tplPath]: { kind: 'file', text: `name: FileCached\nlabels: [registry-bot:fcached]\nbody: []\n` },
    },
    resourceBotConfig: {
      requests: { fcached: { issueTemplate: 'templates/fcached.yml' } },
    },
  });

  // First call: fetches file, caches it
  await loadTemplate(context as any, { owner, repo, templatePath: 'templates/fcached.yml' });

  (context.log.debug as jest.MockedFunction<any>).mockClear();

  // Second call with same templatePath → fetchFile hits the file cache → fires L402
  await loadTemplate(context as any, { owner, repo, templatePath: 'templates/fcached.yml' });

  const debugCalls = (context.log.debug as jest.MockedFunction<any>).mock.calls;
  const msgs = debugCalls.map(([, msg]: [unknown, string]) => msg);
  expect(msgs).toContain('tpl:file-cache-hit');

  // getContent should only have been called once (second call uses file cache)
  expect(getContent).toHaveBeenCalledTimes(1);
});
