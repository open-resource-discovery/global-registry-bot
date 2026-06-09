type LoggerLike = {
  warn?: (obj: unknown, msg?: string) => void;
};

type RequestConfigEntry = {
  folderName?: string;
  [k: string]: unknown;
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

type FormData = Record<string, string>;

type ValidationContext = {
  octokit: {
    repos: {
      getContent: (args: { owner: string; repo: string; path: string }) => Promise<unknown>;
    };
  };
  log?: LoggerLike;
};

export async function collectDuplicateRegistryErrors(args: {
  context: ValidationContext;
  owner: string;
  repo: string;
  template: TemplateLike;
  requestCfg: RequestConfigEntry;
  normalizedFormData: FormData;
  getHttpStatus: (error: unknown) => number | undefined;
  resolveRegistryRoot: (template: TemplateLike, requestCfg: RequestConfigEntry) => string;
}): Promise<string[]> {
  const namespace = String(args.normalizedFormData.namespace || '').trim();
  if (!namespace) return [];

  const resourceName = String(args.normalizedFormData.identifier || args.normalizedFormData.namespace || '').trim();
  if (!resourceName) return [];

  try {
    const structRoot = args.resolveRegistryRoot(args.template, args.requestCfg);
    const filePath = `${structRoot}/${resourceName}.yaml`;

    await args.context.octokit.repos.getContent({ owner: args.owner, repo: args.repo, path: filePath });

    return [`Resource '${resourceName}' already exists in registry`];
  } catch (error: unknown) {
    if (args.getHttpStatus(error) !== 404) {
      args.context.log?.warn?.(
        { err: error instanceof Error ? error.message : String(error) },
        'registry existence check failed'
      );
    }
  }

  return [];
}
