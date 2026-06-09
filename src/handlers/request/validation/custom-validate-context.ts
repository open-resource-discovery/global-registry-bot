type RequestConfigEntry = {
  folderName?: string;
  schema?: string;
  issueTemplate?: string;
  [k: string]: unknown;
};

type LoggerLike = {
  debug?: (obj: unknown, msg?: string) => void;
  info?: (obj: unknown, msg?: string) => void;
  warn?: (obj: unknown, msg?: string) => void;
  error?: (obj: unknown, msg?: string) => void;
};

type RepoRef = { owner: string; repo: string };
type IssueRef = { owner: string; repo: string; issue_number: number };

type RepoContentFile = { content: string; encoding?: string };
type RepoContentResponse = RepoContentFile | RepoContentFile[];
type IssueListItem = { title: string; number: number };

export type OctokitLike = {
  repos: {
    getContent: (args: { owner: string; repo: string; path: string }) => Promise<{ data: RepoContentResponse }>;
  };
  issues: {
    get: (args: { owner: string; repo: string; issue_number: number }) => Promise<{ data: unknown }>;
    listForRepo: (args: {
      owner: string;
      repo: string;
      state: 'open' | 'closed' | 'all';
      per_page?: number;
    }) => Promise<{ data: IssueListItem[] }>;
    update: (args: {
      owner: string;
      repo: string;
      issue_number: number;
      body?: string;
      state?: 'open' | 'closed';
      title?: string;
    }) => Promise<unknown>;
    create: (args: { owner: string; repo: string; title: string; body: string; labels?: string[] }) => Promise<unknown>;
    createComment: (args: { owner: string; repo: string; issue_number: number; body: string }) => Promise<unknown>;
    addLabels: (args: { owner: string; repo: string; issue_number: number; labels: string[] }) => Promise<unknown>;
    removeLabel: (args: { owner: string; repo: string; issue_number: number; name: string }) => Promise<unknown>;
  };
};

type ValidationContext = {
  octokit: OctokitLike;
  log?: LoggerLike;
  repo: () => RepoRef;
  issue: () => IssueRef;
  [k: string]: unknown;
};

type IssueLike = {
  number?: number;
  body?: string | null;
  title?: string | null;
  state?: string | null;
  labels?: (string | { name?: string | null })[] | null;
  user?: { login?: string | null } | null;
};

type TemplateField = {
  id?: string;
  attributes?: { label?: string };
  validations?: { required?: boolean };
  [k: string]: unknown;
};

type TemplateMeta = {
  requestType?: string;
  schema?: string;
  root?: string;
  path?: string;
  [k: string]: unknown;
};

type TemplateLike = {
  body?: TemplateField[];
  title?: string;
  name?: string;
  _meta?: TemplateMeta;
  [k: string]: unknown;
};

type ValidationHookIssue = Readonly<{
  number: number;
  title: string;
  body: string;
  state: string;
  author: string;
  labels: string[];
}>;

type ValidationHookRequestAuthor = Readonly<{
  id: string;
}>;

type BuildCustomValidateContextArgsResult = {
  requestAuthor: ValidationHookRequestAuthor;
  issue: ValidationHookIssue;
  parentResourceName?: string;
  parentCandidate?: Readonly<Record<string, unknown>> | null;
  parentOwners?: readonly string[];
};

type BuildCustomValidateContextArgsArgs = {
  context: ValidationContext;
  owner: string;
  repo: string;
  issue: IssueLike;
  template: TemplateLike;
  requestCfg: RequestConfigEntry;
  resourceName: string;
  resolveRegistryRootForTemplate: (
    context: ValidationContext,
    template: TemplateLike,
    requestCfg: RequestConfigEntry
  ) => string;
  toStringSafe: (value: unknown) => string;
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
  isRepoContentFile: (value: unknown) => value is RepoContentFile;
  getHttpStatus: (err: unknown) => number | undefined;
};

function approvalIssueLabelName(
  value: unknown,
  toStringSafe: (value: unknown) => string,
  isPlainObject: (value: unknown) => value is Record<string, unknown>
): string {
  if (typeof value === 'string') return toStringSafe(value);
  if (isPlainObject(value)) return toStringSafe(value['name']);
  return '';
}

function toApprovalIssueLabelNames(
  labels: IssueLike['labels'],
  toStringSafe: (value: unknown) => string,
  isPlainObject: (value: unknown) => value is Record<string, unknown>
): string[] {
  const items = Array.isArray(labels) ? labels : [];
  return items.map((label) => approvalIssueLabelName(label, toStringSafe, isPlainObject)).filter(Boolean);
}

function buildValidationHookIssue(
  issue: IssueLike,
  toStringSafe: (value: unknown) => string,
  isPlainObject: (value: unknown) => value is Record<string, unknown>
): ValidationHookIssue {
  const author = toStringSafe(issue?.user?.login);

  return {
    number: typeof issue?.number === 'number' ? issue.number : 0,
    title: toStringSafe(issue?.title),
    body: toStringSafe(issue?.body),
    state: toStringSafe(issue?.state),
    author,
    labels: toApprovalIssueLabelNames(issue?.labels, toStringSafe, isPlainObject),
  };
}

function buildValidationHookRequestAuthor(
  issue: IssueLike,
  toStringSafe: (value: unknown) => string
): ValidationHookRequestAuthor {
  return {
    id: toStringSafe(issue?.user?.login),
  };
}

function resolveUpperNamespaceName(resourceName: unknown, toStringSafe: (value: unknown) => string): string {
  const parts = toStringSafe(resourceName)
    .split('.')
    .map((part) => part.trim())
    .filter(Boolean);

  if (parts.length <= 2) return '';

  return parts.slice(0, -1).join('.');
}

async function parseYamlObject(
  raw: string,
  isPlainObject: (value: unknown) => value is Record<string, unknown>
): Promise<Record<string, unknown> | null> {
  try {
    const yamlMod = (await import('yaml')) as unknown as {
      parse?: (src: string) => unknown;
      default?: { parse?: (src: string) => unknown };
    };

    const parse = yamlMod.parse || yamlMod.default?.parse;
    if (typeof parse !== 'function') return null;

    const parsed = parse(raw);
    return isPlainObject(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

async function readRepoYamlObject(args: {
  context: ValidationContext;
  owner: string;
  repo: string;
  basePath: string;
  isRepoContentFile: (value: unknown) => value is RepoContentFile;
  getHttpStatus: (err: unknown) => number | undefined;
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
}): Promise<Record<string, unknown> | null> {
  for (const ext of ['yaml', 'yml']) {
    try {
      const res = await args.context.octokit.repos.getContent({
        owner: args.owner,
        repo: args.repo,
        path: `${args.basePath}.${ext}`,
      });

      const data = res.data;
      if (Array.isArray(data) || !args.isRepoContentFile(data)) continue;

      const text = Buffer.from(data.content, (data.encoding || 'base64') as BufferEncoding).toString('utf8');
      const parsed = await parseYamlObject(text, args.isPlainObject);
      if (parsed) return parsed;
    } catch (e: unknown) {
      if (args.getHttpStatus(e) === 404) continue;
      throw e;
    }
  }

  return null;
}

async function resolveParentCandidateForValidationHook(args: {
  context: ValidationContext;
  owner: string;
  repo: string;
  template: TemplateLike;
  requestCfg: RequestConfigEntry;
  resourceName: string;
  resolveRegistryRootForTemplate: (
    context: ValidationContext,
    template: TemplateLike,
    requestCfg: RequestConfigEntry
  ) => string;
  toStringSafe: (value: unknown) => string;
  isRepoContentFile: (value: unknown) => value is RepoContentFile;
  getHttpStatus: (err: unknown) => number | undefined;
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
}): Promise<{ parentResourceName: string; parentCandidate: Record<string, unknown> | null }> {
  const parentResourceName = resolveUpperNamespaceName(args.resourceName, args.toStringSafe);
  if (!parentResourceName) {
    return { parentResourceName: '', parentCandidate: null };
  }

  const root = args.resolveRegistryRootForTemplate(args.context, args.template, args.requestCfg);
  const parentCandidate = await readRepoYamlObject({
    context: args.context,
    owner: args.owner,
    repo: args.repo,
    basePath: `${root}/${parentResourceName}`,
    isRepoContentFile: args.isRepoContentFile,
    getHttpStatus: args.getHttpStatus,
    isPlainObject: args.isPlainObject,
  });

  return {
    parentResourceName,
    parentCandidate,
  };
}

function normalizeLoginValue(value: unknown, toStringSafe: (value: unknown) => string): string {
  return toStringSafe(value).replace(/^@+/, '').trim();
}

function addNormalizedOwnerReference(out: Set<string>, value: unknown, toStringSafe: (value: unknown) => string): void {
  const raw = toStringSafe(value);
  if (!raw) return;

  const githubUrlMatch = /(?:github\.com|github\.tools\.sap)\/([A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?)/i.exec(
    raw
  );

  if (githubUrlMatch?.[1]) {
    out.add(normalizeLoginValue(githubUrlMatch[1], toStringSafe).toLowerCase());
  }

  for (const part of raw.split(/[,\s;]+/)) {
    const cleaned = toStringSafe(part).replace(/^@+/, '').toLowerCase();
    if (!cleaned) continue;

    if (cleaned.includes('@')) {
      out.add(cleaned);
      continue;
    }

    if (/^[a-z0-9](?:[a-z0-9-]{0,37}[a-z0-9])?$/i.test(cleaned)) {
      out.add(cleaned);
    }
  }
}

function collectNormalizedOwnerReferences(
  out: Set<string>,
  value: unknown,
  toStringSafe: (value: unknown) => string,
  isPlainObject: (value: unknown) => value is Record<string, unknown>
): void {
  if (value === null || value === undefined) return;

  if (Array.isArray(value)) {
    for (const item of value) collectNormalizedOwnerReferences(out, item, toStringSafe, isPlainObject);
    return;
  }

  if (isPlainObject(value)) {
    for (const item of Object.values(value)) collectNormalizedOwnerReferences(out, item, toStringSafe, isPlainObject);
    return;
  }

  addNormalizedOwnerReference(out, value, toStringSafe);
}

function resolveParentOwnersForValidationHook(
  parentCandidate: Record<string, unknown> | null,
  toStringSafe: (value: unknown) => string,
  isPlainObject: (value: unknown) => value is Record<string, unknown>
): string[] {
  if (!parentCandidate) return [];

  const out = new Set<string>();

  collectNormalizedOwnerReferences(
    out,
    parentCandidate['contacts'] ?? parentCandidate['contact'] ?? parentCandidate['owners'] ?? parentCandidate['owner'],
    toStringSafe,
    isPlainObject
  );

  return Array.from(out).filter(Boolean).sort();
}

export async function buildCustomValidateContextArgs(
  args: BuildCustomValidateContextArgsArgs
): Promise<BuildCustomValidateContextArgsResult> {
  const parent = await resolveParentCandidateForValidationHook({
    context: args.context,
    owner: args.owner,
    repo: args.repo,
    template: args.template,
    requestCfg: args.requestCfg,
    resourceName: args.resourceName,
    resolveRegistryRootForTemplate: args.resolveRegistryRootForTemplate,
    toStringSafe: args.toStringSafe,
    isRepoContentFile: args.isRepoContentFile,
    getHttpStatus: args.getHttpStatus,
    isPlainObject: args.isPlainObject,
  });

  return {
    requestAuthor: buildValidationHookRequestAuthor(args.issue, args.toStringSafe),
    issue: buildValidationHookIssue(args.issue, args.toStringSafe, args.isPlainObject),
    parentResourceName: parent.parentResourceName,
    parentCandidate: parent.parentCandidate,
    parentOwners: resolveParentOwnersForValidationHook(parent.parentCandidate, args.toStringSafe, args.isPlainObject),
  };
}
