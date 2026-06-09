type RequestConfigEntry = {
  folderName?: string;
  schema?: string;
  issueTemplate?: string;
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

type ValidationBuckets = {
  registry: string[];
  form: string[];
  rules: string[];
  schema: string[];
};

export function buildValidateRequestIssueErrorResult<Result, Template extends TemplateLike>(args: {
  errors: string[];
  buckets: ValidationBuckets;
  template: Template;
  schemaObj: unknown;
  message: string;
  targetBucket: string[];
  buildValidateRequestIssueResult: (
    errors: string[],
    buckets: ValidationBuckets,
    template: Template,
    options: {
      schemaObj: unknown;
      ajvErrorsForUnifiedFormat: [];
      formData: FormData;
      namespace: string;
      nsType: string;
    }
  ) => Result;
}): Result {
  args.targetBucket.push(args.message);
  args.errors.push(args.message);

  return args.buildValidateRequestIssueResult(args.errors, args.buckets, args.template, {
    schemaObj: args.schemaObj,
    ajvErrorsForUnifiedFormat: [],
    formData: {},
    namespace: '',
    nsType: '',
  });
}

export function resolveTemplateAndRequestType<Context, Result, Template extends TemplateLike>(args: {
  context: Context;
  template: Template;
  formData: FormData;
  errors: string[];
  buckets: ValidationBuckets;
  getRequestConfig: (context: Context, requestType: unknown) => RequestConfigEntry | null;
  mapPartnerNamespaceRequestTypeToConfigKey: (value: unknown) => string;
  buildValidateRequestIssueResult: (
    errors: string[],
    buckets: ValidationBuckets,
    template: Template,
    options: {
      schemaObj: unknown;
      ajvErrorsForUnifiedFormat: [];
      formData: FormData;
      namespace: string;
      nsType: string;
    }
  ) => Result;
  toStringSafe: (value: unknown) => string;
}): { template: Template; requestType: string; requestCfg: RequestConfigEntry } | { result: Result } {
  let template = args.template;
  let requestType = String(template?._meta?.requestType || '').trim();

  if (requestType && requestType.toLowerCase() === 'partnernamespace') {
    const selected =
      (args.formData as Record<string, unknown>)['requestType'] ??
      (args.formData as Record<string, unknown>)['request-type'];

    const mapped = args.mapPartnerNamespaceRequestTypeToConfigKey(selected);
    if (!mapped) {
      return {
        result: buildValidateRequestIssueErrorResult({
          errors: args.errors,
          buckets: args.buckets,
          template,
          schemaObj: null,
          message: `Invalid Partner Namespace 'Request Type' selection '${args.toStringSafe(selected) || ''}'. Expected one of: authority, system, subContext.`,
          targetBucket: args.buckets.form,
          buildValidateRequestIssueResult: args.buildValidateRequestIssueResult,
        }),
      };
    }

    const mappedCfg = args.getRequestConfig(args.context, mapped);
    if (!mappedCfg) {
      return {
        result: buildValidateRequestIssueErrorResult({
          errors: args.errors,
          buckets: args.buckets,
          template,
          schemaObj: null,
          message: `Configuration error: Partner Namespace selection maps to '${mapped}', but cfg.requests has no such entry.`,
          targetBucket: args.buckets.schema,
          buildValidateRequestIssueResult: args.buildValidateRequestIssueResult,
        }),
      };
    }

    const mappedSchema = args.toStringSafe(mappedCfg.schema);
    if (!mappedSchema) {
      return {
        result: buildValidateRequestIssueErrorResult({
          errors: args.errors,
          buckets: args.buckets,
          template,
          schemaObj: null,
          message: `Configuration error: Partner Namespace selection maps to '${mapped}', but cfg.requests['${mapped}'].schema is empty.`,
          targetBucket: args.buckets.schema,
          buildValidateRequestIssueResult: args.buildValidateRequestIssueResult,
        }),
      };
    }

    const nextMeta = template._meta
      ? {
          ...template._meta,
          requestType: mapped,
          schema: mappedSchema,
          root: args.toStringSafe(mappedCfg.folderName),
        }
      : {
          requestType: mapped,
          schema: mappedSchema,
          root: args.toStringSafe(mappedCfg.folderName),
        };

    template = { ...template };
    template._meta = nextMeta;

    requestType = mapped;
  }

  if (!requestType) {
    return {
      result: buildValidateRequestIssueErrorResult({
        errors: args.errors,
        buckets: args.buckets,
        template,
        schemaObj: null,
        message:
          'Configuration error: template missing _meta.requestType (expected cfg.requests mapping via loadTemplate).',
        targetBucket: args.buckets.schema,
        buildValidateRequestIssueResult: args.buildValidateRequestIssueResult,
      }),
    };
  }

  const requestCfg = args.getRequestConfig(args.context, requestType);
  if (!requestCfg) {
    return {
      result: buildValidateRequestIssueErrorResult({
        errors: args.errors,
        buckets: args.buckets,
        template,
        schemaObj: null,
        message: `Configuration error: unknown requestType '${requestType}' (missing cfg.requests entry).`,
        targetBucket: args.buckets.schema,
        buildValidateRequestIssueResult: args.buildValidateRequestIssueResult,
      }),
    };
  }

  return { template, requestType, requestCfg };
}
