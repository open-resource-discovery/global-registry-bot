/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/* eslint-disable require-await */
import { jest } from '@jest/globals';
import { categoryFromTemplate, loadTemplate, parseForm } from '../src/handlers/request/template.js';

type FileEntry = { kind: 'file'; text: string; encoding?: BufferEncoding } | { kind: 'dir' };

function b64utf8(s: string): string {
  return Buffer.from(s, 'utf8').toString('base64');
}

function mkContext(args: { files: Record<string, FileEntry>; resourceBotConfig?: any }) {
  const getContent = jest.fn(async ({ _owner, _repo, path }: any) => {
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

test('throws if octokit is missing', async () => {
  await expect(loadTemplate({} as any, { owner: 'o', repo: 'r' })).rejects.toThrow('octokit is not available');
});

test('throws if owner/repo missing', async () => {
  const { context } = mkContext({ files: {} });
  await expect(loadTemplate(context as any, { owner: '', repo: 'r' })).rejects.toThrow('owner/repo are required');
  await expect(loadTemplate(context as any, { owner: 'o', repo: '' })).rejects.toThrow('owner/repo are required');
});

test('templatePath: resolves relative path under .github/registry-bot and injects request meta from cfg', async () => {
  const owner = 'o_tplpath';
  const repo = 'r';
  const tplPath = '.github/registry-bot/templates/product.yml';

  const { context, getContent } = mkContext({
    files: {
      [tplPath]: {
        kind: 'file',
        text: `
name: Product Request
title: Add product
labels:
  - registry-bot:product
body:
  - id: productId
    attributes:
      label: Product ID
_meta:
  schema: fromTemplate
  root: fromTemplateRoot
`,
      },
    },
    resourceBotConfig: {
      requests: {
        product: {
          issueTemplate: 'templates/product.yml',
          schema: 'schemas/product.schema.json',
          folderName: 'products',
        },
      },
    },
  });

  const tpl = await loadTemplate(context as any, {
    owner,
    repo,
    templatePath: 'templates/product.yml',
  });

  expect(getContent).toHaveBeenCalledWith({ owner, repo, path: tplPath });

  expect(tpl._meta?.path).toBe(tplPath);
  expect(tpl._meta?.name).toBe('Product Request');

  expect(tpl._meta?.requestType).toBe('product');
  expect(tpl._meta?.schema).toBe('.github/registry-bot/schemas/product.schema.json');
  expect(tpl._meta?.root).toBe('products');

  expect(categoryFromTemplate(tpl)).toBe('product');
});

test('templatePath: throws if path is a directory', async () => {
  const owner = 'o_dir';
  const repo = 'r';
  const tplPath = '.github/registry-bot/templates/dir.yml';

  const { context } = mkContext({
    files: { [tplPath]: { kind: 'dir' } },
  });

  await expect(loadTemplate(context as any, { owner, repo, templatePath: 'templates/dir.yml' })).rejects.toThrow(
    `Template path '${tplPath}' is not a file.`
  );
});

test('yaml template: reads body from attributes.body and labels from attributes.labels', async () => {
  const owner = 'o_attr';
  const repo = 'r';
  const tplPath = '.github/registry-bot/templates/attr.yml';

  const { context } = mkContext({
    files: {
      [tplPath]: {
        kind: 'file',
        text: `
name: Attr Template
attributes:
  labels:
    - registry-bot:attr
  body:
    - id: field
      attributes:
        label: Field
`,
      },
    },
  });

  const tpl = await loadTemplate(context as any, {
    owner,
    repo,
    templatePath: 'templates/attr.yml',
  });

  expect(tpl.labels).toEqual(['registry-bot:attr']);
  expect(Array.isArray(tpl.body)).toBe(true);
  expect(tpl.body.length).toBe(1);
});

test('md template: parses YAML front-matter and uses front-matter body field', async () => {
  const owner = 'o_md';
  const repo = 'r';
  const tplPath = '.github/registry-bot/templates/form.md';

  const { context } = mkContext({
    files: {
      [tplPath]: {
        kind: 'file',
        text: `---
name: Md Template
title: MD Title
labels:
  - registry-bot:md
body:
  - id: desc
    attributes:
      label: Description
_meta:
  schema: fromFm
  root: fromFmRoot
---
# ignored markdown
`,
      },
    },
  });

  const tpl = await loadTemplate(context as any, { owner, repo, templatePath: tplPath });

  expect(tpl.title).toBe('MD Title');
  expect(tpl.labels).toEqual(['registry-bot:md']);
  expect(tpl._meta?.schema).toBe('fromFm');
  expect(tpl._meta?.root).toBe('fromFmRoot');
  expect(tpl.body.length).toBe(1);
});

test('routing (prefixed label): selects template via issue labels and caches requestType result', async () => {
  const owner = 'o_route_cache';
  const repo = 'r';

  const productPath = '.github/registry-bot/templates/product.yml';
  const servicePath = '.github/registry-bot/templates/service.yml';

  const { context, getContent } = mkContext({
    files: {
      [productPath]: {
        kind: 'file',
        text: `
name: Product
labels: [registry-bot:product, x]
body: []
`,
      },
      [servicePath]: {
        kind: 'file',
        text: `
name: Service
labels: [registry-bot:service, y]
body: []
`,
      },
    },
    resourceBotConfig: {
      requests: {
        product: { issueTemplate: 'templates/product.yml', folderName: 'products' },
        service: { issueTemplate: 'templates/service.yml', folderName: 'services' },
      },
    },
  });

  const tpl1 = await loadTemplate(context as any, {
    owner,
    repo,
    issueLabels: ['Registry-Bot:Service'],
    issueTitle: 't',
  });

  expect(tpl1._meta?.requestType).toBe('service');
  expect(tpl1._meta?.root).toBe('services');

  expect(getContent).toHaveBeenCalledTimes(2);

  const tpl2 = await loadTemplate(context as any, {
    owner,
    repo,
    issueLabels: ['registry-bot:service'],
    issueTitle: 't',
  });

  expect(tpl2._meta?.requestType).toBe('service');
  expect(getContent).toHaveBeenCalledTimes(2);
});

test('routing: throws if issue has no routing label', async () => {
  const owner = 'o_no_label';
  const repo = 'r';

  const aPath = '.github/registry-bot/templates/a.yml';
  const bPath = '.github/registry-bot/templates/b.yml';

  const { context } = mkContext({
    files: {
      [aPath]: { kind: 'file', text: `name: A\nlabels: [registry-bot:a]\nbody: []\n` },
      [bPath]: { kind: 'file', text: `name: B\nlabels: [registry-bot:b]\nbody: []\n` },
    },
    resourceBotConfig: {
      requests: {
        a: { issueTemplate: 'templates/a.yml' },
        b: { issueTemplate: 'templates/b.yml' },
      },
    },
  });

  await expect(
    loadTemplate(context as any, { owner, repo, issueLabels: ['something-else'], issueTitle: 't' })
  ).rejects.toThrow('no routing label found on issue');
});

test('routing: throws if issue has multiple routing labels for different requestTypes', async () => {
  const owner = 'o_multi';
  const repo = 'r';

  const aPath = '.github/registry-bot/templates/a.yml';
  const bPath = '.github/registry-bot/templates/b.yml';

  const { context } = mkContext({
    files: {
      [aPath]: { kind: 'file', text: `name: A\nlabels: [registry-bot:a]\nbody: []\n` },
      [bPath]: { kind: 'file', text: `name: B\nlabels: [registry-bot:b]\nbody: []\n` },
    },
    resourceBotConfig: {
      requests: {
        a: { issueTemplate: 'templates/a.yml' },
        b: { issueTemplate: 'templates/b.yml' },
      },
    },
  });

  await expect(
    loadTemplate(context as any, {
      owner,
      repo,
      issueLabels: ['registry-bot:a', 'registry-bot:b'],
      issueTitle: 't',
    })
  ).rejects.toThrow('multiple routing labels');
});

test('label-index: throws if a template has multiple prefixed routing labels', async () => {
  const owner = 'o_bad_prefixed';
  const repo = 'r';

  const badPath = '.github/registry-bot/templates/bad.yml';

  const { context } = mkContext({
    files: {
      [badPath]: {
        kind: 'file',
        text: `
name: Bad
labels: [registry-bot:a, registry-bot:b]
body: []
`,
      },
    },
    resourceBotConfig: {
      requests: {
        bad: { issueTemplate: 'templates/bad.yml' },
      },
    },
  });

  await expect(
    loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:a'], issueTitle: 't' })
  ).rejects.toThrow('must define exactly ONE routing label');
});

test('label-index: unique-label fallback works when no prefixed labels exist', async () => {
  const owner = 'o_unique';
  const repo = 'r';

  const pPath = '.github/registry-bot/templates/p.yml';
  const sPath = '.github/registry-bot/templates/s.yml';

  const { context } = mkContext({
    files: {
      [pPath]: { kind: 'file', text: `name: P\nlabels: [product]\nbody: []\n` },
      [sPath]: { kind: 'file', text: `name: S\nlabels: [service]\nbody: []\n` },
    },
    resourceBotConfig: {
      requests: {
        product: { issueTemplate: 'templates/p.yml' },
        service: { issueTemplate: 'templates/s.yml' },
      },
    },
  });

  const tpl = await loadTemplate(context as any, {
    owner,
    repo,
    issueLabels: ['service'],
    issueTitle: 't',
  });

  expect(tpl._meta?.requestType).toBe('service');
});

test('label-index: throws if no prefixed labels and no unique labels exist', async () => {
  const owner = 'o_no_unique';
  const repo = 'r';

  const aPath = '.github/registry-bot/templates/a.yml';
  const bPath = '.github/registry-bot/templates/b.yml';

  const { context } = mkContext({
    files: {
      [aPath]: { kind: 'file', text: `name: A\nlabels: [common]\nbody: []\n` },
      [bPath]: { kind: 'file', text: `name: B\nlabels: [common]\nbody: []\n` },
    },
    resourceBotConfig: {
      requests: {
        a: { issueTemplate: 'templates/a.yml' },
        b: { issueTemplate: 'templates/b.yml' },
      },
    },
  });

  await expect(loadTemplate(context as any, { owner, repo, issueLabels: ['common'], issueTitle: 't' })).rejects.toThrow(
    'Cannot resolve routing label'
  );
});

test('parseForm wrapper: filters invalid body fields and forwards to parser', () => {
  const tpl: any = {
    labels: [],
    body: [
      { id: 'field', attributes: { label: 'Field' } },
      { id: 123 },
      { attributes: { label: 'NoId' } },
      { id: 'bad', attributes: { label: 1 } },
    ],
    _meta: { path: 'x', name: 'y' },
  };

  const out = parseForm('## Field\nvalue', tpl);
  expect(out).toEqual({ field: 'value' });
});
