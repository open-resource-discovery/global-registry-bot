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

// ─── categoryFromTemplate edge cases ────────────────────────────────────────

describe('categoryFromTemplate', () => {
  it('returns empty string for null template', () => {
    expect(categoryFromTemplate(null)).toBe('');
  });

  it('returns empty string for undefined template', () => {
    expect(categoryFromTemplate(undefined)).toBe('');
  });

  it('returns empty string when _meta has no requestType', () => {
    const tpl: any = { labels: [], body: [], _meta: { path: 'p', name: 'n' } };
    expect(categoryFromTemplate(tpl)).toBe('');
  });

  it('trims whitespace from requestType', () => {
    const tpl: any = { labels: [], body: [], _meta: { path: 'p', name: 'n', requestType: '  product  ' } };
    expect(categoryFromTemplate(tpl)).toBe('product');
  });
});

// ─── parseForm with empty/null body ─────────────────────────────────────────

describe('parseForm edge cases', () => {
  it('returns empty object when template body is not an array', () => {
    const tpl: any = { labels: [], body: 'not-array' };
    const out = parseForm('## Field\nvalue', tpl);
    expect(out).toEqual({});
  });

  it('returns empty object when body is an empty string', () => {
    const tpl: any = { labels: [], body: [{ id: 'f', attributes: { label: 'F' } }] };
    const out = parseForm('', tpl);
    expect(out).toEqual({});
  });

  it('filters body entries where attributes is not a plain object', () => {
    // id is valid string but attributes is an array — should be excluded
    const tpl: any = {
      labels: [],
      body: [
        { id: 'good', attributes: { label: 'Good' } },
        { id: 'bad-attr', attributes: ['not', 'plain'] },
      ],
    };
    const out = parseForm('## Good\nval', tpl);
    expect(out).toEqual({ good: 'val' });
  });

  it('filters null body entries — isTemplateField(null) returns false (L557 arm0)', () => {
    const tpl: any = {
      labels: [],
      body: [null, { id: 'f', attributes: { label: 'F' } }],
    };
    const out = parseForm('## F\nval', tpl);
    expect(out).toEqual({ f: 'val' });
  });
});

// ─── resolveRepoPathFromConfig via templatePath ──────────────────────────────

describe('templatePath resolution branches', () => {
  it('templatePath starting with / is resolved as absolute (leading slash stripped)', async () => {
    const owner = 'o_abs';
    const repo = 'r_abs';
    const tplPath = '.github/ISSUE_TEMPLATE/product.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          text: `name: Abs\ntitle: T\nlabels: [registry-bot:abs]\nbody: []\n`,
        },
      },
    });

    // Leading slash should be stripped and path is used as-is (absolute from root)
    const tpl = await loadTemplate(context as any, {
      owner,
      repo,
      templatePath: '/.github/ISSUE_TEMPLATE/product.yml',
    });
    expect(tpl._meta?.path).toBe(tplPath);
  });

  it('templatePath already starting with .github/ is not re-prefixed', async () => {
    const owner = 'o_dotgithub';
    const repo = 'r_dotgithub';
    const tplPath = '.github/ISSUE_TEMPLATE/direct.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          text: `name: Direct\ntitle: T\nlabels: [registry-bot:direct]\nbody: []\n`,
        },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, templatePath: '.github/ISSUE_TEMPLATE/direct.yml' });
    expect(tpl._meta?.path).toBe(tplPath);
  });

  it('templatePath with .. segments is normalized', async () => {
    const owner = 'o_dotdot';
    const repo = 'r_dotdot';
    // templates/../templates/product.yml → .github/registry-bot/templates/product.yml
    const tplPath = '.github/registry-bot/templates/product.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          text: `name: DotDot\ntitle: T\nlabels: [registry-bot:dd]\nbody: []\n`,
        },
      },
    });

    const tpl = await loadTemplate(context as any, {
      owner,
      repo,
      templatePath: 'templates/../templates/product.yml',
    });
    expect(tpl._meta?.path).toBe(tplPath);
  });

  it('templatePath with only whitespace is treated as missing (routes via label index)', async () => {
    const owner = 'o_ws_tpl';
    const repo = 'r_ws_tpl';
    const tplPath = '.github/registry-bot/templates/ws.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          text: `name: WS\nlabels: [registry-bot:ws]\nbody: []\n`,
        },
      },
      resourceBotConfig: {
        requests: { ws: { issueTemplate: 'templates/ws.yml', folderName: 'ws-folder' } },
      },
    });

    // Whitespace-only templatePath should be treated as absent and fall through to label routing
    const tpl = await loadTemplate(context as any, {
      owner,
      repo,
      templatePath: '   ',
      issueLabels: ['registry-bot:ws'],
    });
    expect(tpl._meta?.requestType).toBe('ws');
  });
});

// ─── getRoutingLabelPrefixes variations ─────────────────────────────────────

describe('routing label prefix config variations', () => {
  it('uses routingLabelPrefixes (plural) when set as array', async () => {
    const owner = 'o_prefixes_arr';
    const repo = 'r_prefixes_arr';
    const tplPath = '.github/registry-bot/templates/x.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          text: `name: X\nlabels: [custom:x]\nbody: []\n`,
        },
      },
      resourceBotConfig: {
        workflow: { labels: { routingLabelPrefixes: ['custom:'] } },
        requests: { x: { issueTemplate: 'templates/x.yml' } },
      },
    });

    const tpl = await loadTemplate(context as any, {
      owner,
      repo,
      issueLabels: ['custom:x'],
    });
    expect(tpl._meta?.requestType).toBe('x');
  });

  it('uses routingLabelPrefix (singular) as string', async () => {
    const owner = 'o_prefix_str';
    const repo = 'r_prefix_str';
    const tplPath = '.github/registry-bot/templates/y.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          text: `name: Y\nlabels: [req:y]\nbody: []\n`,
        },
      },
      resourceBotConfig: {
        workflow: { labels: { routingLabelPrefix: 'req:' } },
        requests: { y: { issueTemplate: 'templates/y.yml' } },
      },
    });

    const tpl = await loadTemplate(context as any, {
      owner,
      repo,
      issueLabels: ['req:y'],
    });
    expect(tpl._meta?.requestType).toBe('y');
  });

  it('falls back to default registry-bot: prefix when no labels config exists', async () => {
    const owner = 'o_no_wf';
    const repo = 'r_no_wf';
    const tplPath = '.github/registry-bot/templates/nw.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: { kind: 'file', text: `name: NW\nlabels: [registry-bot:nw]\nbody: []\n` },
      },
      resourceBotConfig: {
        requests: { nw: { issueTemplate: 'templates/nw.yml' } },
        // no workflow key at all
      },
    });

    const tpl = await loadTemplate(context as any, {
      owner,
      repo,
      issueLabels: ['registry-bot:nw'],
    });
    expect(tpl._meta?.requestType).toBe('nw');
  });

  it('falls back to default prefix when workflow.labels is not a plain object', async () => {
    const owner = 'o_bad_wf_labels';
    const repo = 'r_bad_wf_labels';
    const tplPath = '.github/registry-bot/templates/bw.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: { kind: 'file', text: `name: BW\nlabels: [registry-bot:bw]\nbody: []\n` },
      },
      resourceBotConfig: {
        workflow: { labels: 'bad-value' },
        requests: { bw: { issueTemplate: 'templates/bw.yml' } },
      },
    });

    const tpl = await loadTemplate(context as any, {
      owner,
      repo,
      issueLabels: ['registry-bot:bw'],
    });
    expect(tpl._meta?.requestType).toBe('bw');
  });
});

// ─── toLabelStrings variations ───────────────────────────────────────────────

describe('toLabelStrings / issue label formats', () => {
  it('handles label objects with name property', async () => {
    const owner = 'o_label_obj';
    const repo = 'r_label_obj';
    const tplPath = '.github/registry-bot/templates/obj.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: { kind: 'file', text: `name: Obj\nlabels: [registry-bot:obj]\nbody: []\n` },
      },
      resourceBotConfig: {
        requests: { obj: { issueTemplate: 'templates/obj.yml' } },
      },
    });

    // GitHub API returns labels as objects with { name: string }
    const tpl = await loadTemplate(context as any, {
      owner,
      repo,
      issueLabels: [{ name: 'registry-bot:obj' }, { name: 'other' }],
    });
    expect(tpl._meta?.requestType).toBe('obj');
  });

  it('skips null/undefined label entries', async () => {
    const owner = 'o_null_label';
    const repo = 'r_null_label';
    const tplPath = '.github/registry-bot/templates/nl.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: { kind: 'file', text: `name: NL\nlabels: [registry-bot:nl]\nbody: []\n` },
      },
      resourceBotConfig: {
        requests: { nl: { issueTemplate: 'templates/nl.yml' } },
      },
    });

    const tpl = await loadTemplate(context as any, {
      owner,
      repo,
      // mix of null, undefined, valid string
      issueLabels: [null, undefined, 'registry-bot:nl'] as any[],
    });
    expect(tpl._meta?.requestType).toBe('nl');
  });

  it('returns empty labels when issueLabels is not an array', async () => {
    const owner = 'o_non_arr';
    const repo = 'r_non_arr';
    const tplPath = '.github/registry-bot/templates/na.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: { kind: 'file', text: `name: NA\nlabels: [registry-bot:na]\nbody: []\n` },
      },
      resourceBotConfig: {
        requests: { na: { issueTemplate: 'templates/na.yml' } },
      },
    });

    // issueLabels not an array → toLabelStrings returns [] → no routing label → throws
    await expect(loadTemplate(context as any, { owner, repo, issueLabels: 'not-an-array' as any })).rejects.toThrow(
      'no routing label found on issue'
    );
  });
});

// ─── getRequestsConfig non-object ───────────────────────────────────────────

describe('getRequestsConfig edge cases', () => {
  it('returns empty object when requests is not a plain object', async () => {
    const owner = 'o_req_str';
    const repo = 'r_req_str';

    const { context } = mkContext({
      files: {},
      resourceBotConfig: { requests: 'bad' },
    });

    // With empty requests config, issueLabels routing fails
    await expect(loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:x'] })).rejects.toThrow(
      'no routing label found on issue'
    );
  });
});

// ─── parseFrontMatterMd edge cases ──────────────────────────────────────────

describe('parseFrontMatterMd edge cases', () => {
  it('md template with no front-matter returns empty labels and body', async () => {
    const owner = 'o_no_fm';
    const repo = 'r_no_fm';
    const tplPath = '.github/registry-bot/templates/nofm.md';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          text: `# Just a markdown header\nNo front-matter here.\n`,
        },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, templatePath: tplPath });
    expect(tpl.labels).toEqual([]);
    expect(tpl.body).toEqual([]);
  });

  it('md template with invalid YAML front-matter falls back to empty parsed', async () => {
    const owner = 'o_bad_fm';
    const repo = 'r_bad_fm';
    const tplPath = '.github/registry-bot/templates/badfm.md';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          // front-matter is present but YAML parse will produce a scalar (string), not object
          text: `---\njust a scalar string\n---\nbody content\n`,
        },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, templatePath: tplPath });
    // fm is null because parsed is not a plain object → body is []
    expect(tpl.labels).toEqual([]);
    expect(tpl.body).toEqual([]);
  });
});

// ─── YAML template body/labels resolution branches ──────────────────────────

describe('YAML template body resolution branches', () => {
  it('uses top-level labels when both top-level and attributes.labels are present', async () => {
    const owner = 'o_top_labels';
    const repo = 'r_top_labels';
    const tplPath = '.github/registry-bot/templates/top.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          text: `
name: Top Labels
labels:
  - registry-bot:top
attributes:
  labels:
    - ignored-label
body:
  - id: f1
    attributes:
      label: Field One
`,
        },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, templatePath: tplPath });
    // Top-level labels takes priority over attributes.labels
    expect(tpl.labels).toEqual(['registry-bot:top']);
  });

  it('uses empty body array when neither body nor attributes.body is present', async () => {
    const owner = 'o_no_body';
    const repo = 'r_no_body';
    const tplPath = '.github/registry-bot/templates/nobody.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          text: `name: No Body\nlabels: [registry-bot:nb]\n`,
        },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, templatePath: tplPath });
    expect(tpl.body).toEqual([]);
  });

  it('resolves name from attributes.name when top-level name is absent', async () => {
    const owner = 'o_attr_name';
    const repo = 'r_attr_name';
    const tplPath = '.github/registry-bot/templates/attrname.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          text: `
attributes:
  name: From Attributes
  labels:
    - registry-bot:an
  body: []
`,
        },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, templatePath: tplPath });
    expect(tpl._meta?.name).toBe('From Attributes');
  });

  it('uses top-level schema/root over _meta fallbacks', async () => {
    const owner = 'o_top_schema';
    const repo = 'r_top_schema';
    const tplPath = '.github/registry-bot/templates/topschema.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          text: `
name: Top Schema
labels: [registry-bot:ts]
schema: top-schema.json
root: top-root
body: []
_meta:
  schema: meta-schema.json
  root: meta-root
`,
        },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, templatePath: tplPath });
    // _meta.schema takes priority over top-level schema (metaSchemaFallback || schemaTop)
    expect(tpl._meta?.schema).toBe('meta-schema.json');
    expect(tpl._meta?.root).toBe('meta-root');
  });

  it('falls back to top-level schema/root when _meta has none', async () => {
    const owner = 'o_fallback_schema';
    const repo = 'r_fallback_schema';
    const tplPath = '.github/registry-bot/templates/fbschema.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          text: `
name: Fallback Schema
labels: [registry-bot:fb]
schema: fallback-schema.json
root: fallback-root
body: []
`,
        },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, templatePath: tplPath });
    expect(tpl._meta?.schema).toBe('fallback-schema.json');
    expect(tpl._meta?.root).toBe('fallback-root');
  });

  it('invalid YAML in yml template falls back to empty parsed object', async () => {
    const owner = 'o_bad_yaml';
    const repo = 'r_bad_yaml';
    const tplPath = '.github/registry-bot/templates/bad.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          // YAML that parses to a non-plain-object (scalar string)
          text: `just a plain string`,
        },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, templatePath: tplPath });
    expect(tpl.labels).toEqual([]);
    expect(tpl.body).toEqual([]);
  });

  it('YAML parse throws on malformed YAML → catch block sets parsed = {} (L432)', async () => {
    const owner = 'o_throw_yaml';
    const repo = 'r_throw_yaml';
    const tplPath = '.github/registry-bot/templates/throw.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          // unclosed bracket causes YAML.parse to throw
          text: `name: Bad\nlabels: [unclosed bracket\nbody: []`,
        },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, templatePath: tplPath });
    expect(tpl.labels).toEqual([]);
    expect(tpl.body).toEqual([]);
  });
});

// ─── applyRequestMeta branches ───────────────────────────────────────────────

describe('applyRequestMeta branches', () => {
  it('requestType resolves via label when rc has no schema (schemaPath is undefined)', async () => {
    const owner = 'o_no_schema_rc';
    const repo = 'r_no_schema_rc';
    const tplPath = '.github/registry-bot/templates/ns.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: { kind: 'file', text: `name: NS\nlabels: [registry-bot:ns]\nbody: []\n` },
      },
      resourceBotConfig: {
        requests: {
          ns: {
            issueTemplate: 'templates/ns.yml',
            folderName: 'ns-folder',
            // no schema field
          },
        },
      },
    });

    const tpl = await loadTemplate(context as any, {
      owner,
      repo,
      issueLabels: ['registry-bot:ns'],
    });
    expect(tpl._meta?.schema).toBeUndefined();
    expect(tpl._meta?.requestType).toBe('ns');
  });
});

// ─── buildLabelIndexFromTemplates: ambiguous unique labels ───────────────────

describe('label-index: ambiguous multiple-unique-label error', () => {
  it('throws when template has multiple unique labels and no prefixed labels', async () => {
    const owner = 'o_multi_unique';
    const repo = 'r_multi_unique';

    const aPath = '.github/registry-bot/templates/mu_a.yml';
    const bPath = '.github/registry-bot/templates/mu_b.yml';

    // a has two unique labels (not shared with b), so routing is ambiguous
    const { context } = mkContext({
      files: {
        [aPath]: { kind: 'file', text: `name: MUA\nlabels: [unique-a1, unique-a2]\nbody: []\n` },
        [bPath]: { kind: 'file', text: `name: MUB\nlabels: [unique-b]\nbody: []\n` },
      },
      resourceBotConfig: {
        requests: {
          a: { issueTemplate: 'templates/mu_a.yml' },
          b: { issueTemplate: 'templates/mu_b.yml' },
        },
      },
    });

    await expect(
      loadTemplate(context as any, { owner, repo, issueLabels: ['unique-a1'], issueTitle: 't' })
    ).rejects.toThrow('has multiple unique labels and routing would be ambiguous');
  });
});

// ─── label-index: duplicate routing label across two request types ────────────

describe('label-index: duplicate routing label collision', () => {
  it('throws when same routing label is used by two different requestTypes', async () => {
    const owner = 'o_dup_routing';
    const repo = 'r_dup_routing';

    const aPath = '.github/registry-bot/templates/dup_a.yml';
    const bPath = '.github/registry-bot/templates/dup_b.yml';

    const { context } = mkContext({
      files: {
        // Both templates share the same prefixed routing label
        [aPath]: { kind: 'file', text: `name: DupA\nlabels: [registry-bot:shared]\nbody: []\n` },
        [bPath]: { kind: 'file', text: `name: DupB\nlabels: [registry-bot:shared]\nbody: []\n` },
      },
      resourceBotConfig: {
        requests: {
          typeA: { issueTemplate: 'templates/dup_a.yml' },
          typeB: { issueTemplate: 'templates/dup_b.yml' },
        },
      },
    });

    await expect(
      loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:shared'], issueTitle: 't' })
    ).rejects.toThrow('routing label');
  });
});

// ─── label-index: missing issueTemplate in config ────────────────────────────

describe('label-index: missing issueTemplate throws', () => {
  it('throws configuration error when issueTemplate is missing from config entry', async () => {
    const owner = 'o_missing_tpl';
    const repo = 'r_missing_tpl';

    const { context } = mkContext({
      files: {},
      resourceBotConfig: {
        requests: {
          broken: { folderName: 'somewhere' /* no issueTemplate */ },
        },
      },
    });

    await expect(
      loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:broken'], issueTitle: 't' })
    ).rejects.toThrow('issueTemplate is missing');
  });
});

// ─── file-cache hit (second fetch of same path in same owner/repo) ───────────

describe('template file cache', () => {
  it('uses cached file on second fetch for same path and owner/repo', async () => {
    const owner = 'o_filecache';
    const repo = 'r_filecache';
    const tplPath = '.github/registry-bot/templates/cached.yml';

    const { context, getContent } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          text: `name: Cached\nlabels: [registry-bot:cached]\nbody: []\n`,
        },
      },
      resourceBotConfig: {
        requests: { cached: { issueTemplate: 'templates/cached.yml' } },
      },
    });

    // First call: builds label index (fetches template) + fetches for requestType
    await loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:cached'] });
    const callCount = getContent.mock.calls.length;

    // Second call for the same path via templatePath (should use file cache, not re-fetch)
    await loadTemplate(context as any, { owner, repo, templatePath: 'templates/cached.yml' });
    // File cache hit means getContent call count does NOT increase
    expect(getContent.mock.calls.length).toBe(callCount);
  });
});

// ─── requestType and label-index cache hits ───────────────────────────────────

describe('cache: requestType and label-index cache hit on second call', () => {
  it('returns same template from cache on second label-based loadTemplate call', async () => {
    const owner = 'o_rt_cache';
    const repo = 'r_rt_cache';
    const tplPath = '.github/registry-bot/templates/cached_rt.yml';

    const { context, getContent } = mkContext({
      files: {
        [tplPath]: {
          kind: 'file',
          text: `name: CachedRt\nlabels: [registry-bot:cached-rt]\nbody: []\n`,
        },
      },
      resourceBotConfig: {
        requests: { cachedRt: { issueTemplate: 'templates/cached_rt.yml' } },
      },
    });

    const tpl1 = await loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:cached-rt'] });
    const fetchCount = getContent.mock.calls.length;
    const tpl2 = await loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:cached-rt'] });

    expect(tpl2._meta?.requestType).toBe(tpl1._meta?.requestType);
    expect(getContent.mock.calls.length).toBe(fetchCount);
  });
});

// ─── no resourceBotConfig in context ────────────────────────────────────────

describe('no resourceBotConfig in context', () => {
  it('loads template by templatePath when context has no resourceBotConfig', async () => {
    const owner = 'o_no_cfg';
    const repo = 'r_no_cfg';
    const tplPath = '.github/registry-bot/templates/nocfg.yml';

    const getContent = jest.fn(async ({ path }: any) => {
      if (path !== tplPath) throw new Error(`unexpected path: ${path}`);
      return {
        data: {
          content: Buffer.from(`name: NoCfg\nlabels: [x]\nbody: []\n`, 'utf8').toString('base64'),
          encoding: 'base64',
        },
      };
    });

    const ctx = { octokit: { repos: { getContent } }, log: { debug: jest.fn() } };
    const tpl = await loadTemplate(ctx as any, { owner, repo, templatePath: 'templates/nocfg.yml' });
    expect(tpl._meta?.name).toBe('NoCfg');
  });
});

// ─── non-string encoding in API response ─────────────────────────────────────

describe('non-string encoding in API response', () => {
  it('falls back to base64 when encoding field is a number', async () => {
    const owner = 'o_enc';
    const repo = 'r_enc';
    const tplPath = '.github/registry-bot/templates/enc.yml';
    const rawText = `name: EncTest\nlabels: [registry-bot:enc]\nbody: []\n`;

    const getContent = jest.fn(async ({ path }: any) => {
      if (path !== tplPath) throw new Error(`unexpected path: ${path}`);
      return {
        data: {
          content: Buffer.from(rawText, 'utf8').toString('base64'),
          encoding: 42,
        },
      };
    });

    const ctx = {
      octokit: { repos: { getContent } },
      resourceBotConfig: { requests: { enc: { issueTemplate: 'templates/enc.yml' } } },
      log: { debug: jest.fn() },
    };
    const tpl = await loadTemplate(ctx as any, { owner, repo, issueLabels: ['registry-bot:enc'] });
    expect(tpl._meta?.name).toBe('EncTest');
  });
});

// ─── non-string title in YAML template ───────────────────────────────────────

describe('non-string title in YAML template', () => {
  it('coerces numeric title to string', async () => {
    const owner = 'o_numtitle';
    const repo = 'r_numtitle';
    const tplPath = '.github/registry-bot/templates/numtitle.yml';
    const rawText = `name: NumTitle\ntitle: 42\nlabels: [registry-bot:numtitle]\nbody: []\n`;

    const { context } = mkContext({
      files: {
        [tplPath]: { kind: 'file', text: rawText },
      },
      resourceBotConfig: {
        requests: { numtitle: { issueTemplate: 'templates/numtitle.yml' } },
      },
    });

    const tpl = await loadTemplate(context as any, {
      owner,
      repo,
      issueLabels: ['registry-bot:numtitle'],
    });
    expect(tpl.title).toBe('42');
  });
});

// ─── non-string _meta fields in template ─────────────────────────────────────

describe('non-string _meta fields in template', () => {
  it('handles non-string _meta.name, .schema, .root gracefully', async () => {
    const owner = 'o_metafields';
    const repo = 'r_metafields';
    const tplPath = '.github/registry-bot/templates/metafields.yml';
    const rawText = [
      `name: MetaFields`,
      `labels: [registry-bot:metafields]`,
      `_meta:`,
      `  name: 99`,
      `  schema: true`,
      `  root: 5`,
      `body: []`,
    ].join('\n');

    const { context } = mkContext({
      files: { [tplPath]: { kind: 'file', text: rawText } },
      resourceBotConfig: {
        requests: { metafields: { issueTemplate: 'templates/metafields.yml' } },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:metafields'] });
    expect(typeof tpl._meta?.name).toBe('string');
    expect(tpl._meta?.schema).toBeUndefined();
    expect(tpl._meta?.root).toBeUndefined();
  });
});

// ─── YAML template with body under attributes ─────────────────────────────────

describe('YAML template with body under attributes', () => {
  it('uses attributes.body when top-level body is absent', async () => {
    const owner = 'o_attrbody';
    const repo = 'r_attrbody';
    const tplPath = '.github/registry-bot/templates/attrbody.yml';
    const rawText = [
      `name: AttrBody`,
      `labels: [registry-bot:attrbody]`,
      `attributes:`,
      `  body:`,
      `    - id: my-field`,
      `      type: input`,
      `      attributes:`,
      `        label: My Field`,
    ].join('\n');

    const { context } = mkContext({
      files: { [tplPath]: { kind: 'file', text: rawText } },
      resourceBotConfig: {
        requests: { attrbody: { issueTemplate: 'templates/attrbody.yml' } },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:attrbody'] });
    expect(Array.isArray(tpl.body)).toBe(true);
    expect((tpl.body as any[])[0].id).toBe('my-field');
  });
});

// ─── YAML template with no body anywhere ─────────────────────────────────────

describe('YAML template with no body', () => {
  it('results in empty body array when no body or attributes.body', async () => {
    const owner = 'o_nobody';
    const repo = 'r_nobody';
    const tplPath = '.github/registry-bot/templates/nobody.yml';
    const rawText = `name: Nobody\nlabels: [registry-bot:nobody]\n`;

    const { context } = mkContext({
      files: { [tplPath]: { kind: 'file', text: rawText } },
      resourceBotConfig: {
        requests: { nobody: { issueTemplate: 'templates/nobody.yml' } },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:nobody'] });
    expect(tpl.body).toEqual([]);
  });
});

// ─── labels from attributes fallback ─────────────────────────────────────────

describe('template with labels under attributes fallback', () => {
  it('uses attributes.labels when top-level labels absent', async () => {
    const owner = 'o_attrlabels';
    const repo = 'r_attrlabels';
    const tplPath = '.github/registry-bot/templates/attrlabels.yml';
    const rawText = [`name: AttrLabels`, `attributes:`, `  labels: [registry-bot:attrlabels]`, `  body: []`].join('\n');

    const { context } = mkContext({
      files: { [tplPath]: { kind: 'file', text: rawText } },
      resourceBotConfig: {
        requests: { attrlabels: { issueTemplate: 'templates/attrlabels.yml' } },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:attrlabels'] });
    expect(tpl.labels).toContain('registry-bot:attrlabels');
  });
});

// ─── applyRequestMeta: config entry without schema or folderName ──────────────

describe('applyRequestMeta with rc missing optional fields', () => {
  it('leaves schema undefined and root unchanged when rc has neither', async () => {
    const owner = 'o_noschema';
    const repo = 'r_noschema';
    const tplPath = '.github/registry-bot/templates/noschema.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: { kind: 'file', text: `name: NoSchema\nlabels: [registry-bot:noschema]\nbody: []\n` },
      },
      resourceBotConfig: {
        requests: { noschema: { issueTemplate: 'templates/noschema.yml' } },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:noschema'] });
    expect(tpl._meta?.schema).toBeUndefined();
    expect(tpl._meta?.root).toBeUndefined();
  });
});

// ─── findRequestByTemplatePath: config entry without issueTemplate ────────────

describe('findRequestByTemplatePath skips entry without issueTemplate', () => {
  it('loads by templatePath without crashing when another config entry lacks issueTemplate', async () => {
    const owner = 'o_noissue';
    const repo = 'r_noissue';
    const tplPath = '.github/registry-bot/templates/withissue.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: { kind: 'file', text: `name: WithIssue\nlabels: [registry-bot:withissue]\nbody: []\n` },
      },
      resourceBotConfig: {
        requests: {
          withissue: { issueTemplate: 'templates/withissue.yml' },
          broken: { folderName: 'somewhere' },
        },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, templatePath: 'templates/withissue.yml' });
    expect(tpl._meta?.name).toBe('WithIssue');
  });
});

// ─── loadTemplate: issueTitle is optional (opts.issueTitle undefined) ─────────

describe('loadTemplate: undefined issueTitle', () => {
  it('succeeds when issueTitle is not provided', async () => {
    const owner = 'o_notitle';
    const repo = 'r_notitle';
    const tplPath = '.github/registry-bot/templates/notitle.yml';

    const { context } = mkContext({
      files: {
        [tplPath]: { kind: 'file', text: `name: NoTitle\nlabels: [registry-bot:notitle]\nbody: []\n` },
      },
      resourceBotConfig: {
        requests: { notitle: { issueTemplate: 'templates/notitle.yml' } },
      },
    });

    const tpl = await loadTemplate(context as any, { owner, repo, issueLabels: ['registry-bot:notitle'] });
    expect(tpl._meta?.name).toBe('NoTitle');
  });
});

// ─── invalid YAML in .md front matter ────────────────────────────────────────

describe('parseForm and parseFrontMatterMd with invalid YAML', () => {
  it('treats .md template with invalid front matter YAML as empty form', async () => {
    const owner = 'o_badfm';
    const repo = 'r_badfm';
    const tplPath = '.github/registry-bot/templates/badfm.md';
    const rawText = `---\n: invalid: yaml: [\n---\nbody content here\n`;

    const getContent = jest.fn(async ({ path }: any) => {
      if (path !== tplPath) throw new Error(`unexpected: ${path}`);
      return {
        data: {
          content: Buffer.from(rawText, 'utf8').toString('base64'),
          encoding: 'base64',
        },
      };
    });

    const ctx = {
      octokit: { repos: { getContent } },
      resourceBotConfig: {},
      log: { debug: jest.fn() },
    };

    const tpl = await loadTemplate(ctx as any, { owner: owner, repo, templatePath: 'templates/badfm.md' });
    expect(tpl.body).toEqual([]);
  });
});
