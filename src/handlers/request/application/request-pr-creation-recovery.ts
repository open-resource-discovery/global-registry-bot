import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type IssueParamsBase = RepoInfoBase & {
  issue_number: number;
};

type IssueLikeBase = {
  number: number;
};

type TemplateMetaLikeBase = {
  root?: string | null;
};

type TemplateLikeBase = {
  _meta?: TemplateMetaLikeBase;
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type ContextWithOctokit = {
  octokit: {
    repos: {
      getContent: (args: { owner: string; repo: string; path: string }) => Promise<{ data?: unknown }>;
    };
    git: {
      deleteRef: (args: { owner: string; repo: string; ref: string }) => Promise<unknown>;
    };
  };
};

export type RequestPrCreationRecoveryCallbacks<
  ContextType,
  RepoType extends RepoInfoBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
> = {
  createRequestPr: (
    context: ContextType,
    repoInfo: RepoType,
    issue: IssueType,
    parsedFormData: FormDataType,
    options: { template: TemplateType }
  ) => Promise<{ number: number }>;
  getHttpStatus: (error: unknown) => number | undefined;
  renderConfiguredRequestBranchName: (context: ContextType, issue: IssueType, resourceName: string) => string;
};

function resolveStructuredRootForTemplate(template: TemplateLikeBase): string {
  return toStringTrim(template?._meta?.root).replace(/^\/+/, '').replace(/\/+$/, '');
}

function extractCreatePrFailureMessage(error: unknown): string {
  const raw = (error instanceof Error ? error.message : String(error)).trim();
  const withoutUrl = raw.replace(/\s*-\s*https?:\/\/\S+$/i, '').trim();

  const marker = 'Validation Failed:';
  const idx = withoutUrl.indexOf(marker);

  if (idx >= 0) {
    const tail = withoutUrl.slice(idx + marker.length).trim();

    try {
      const parsed = JSON.parse(tail) as Record<string, unknown>;
      const msg = toStringTrim(parsed['message']);
      if (msg) return msg;
    } catch {
      // ignore
    }

    return tail || withoutUrl;
  }

  return withoutUrl;
}

function parseNoCommitsHeadBranchFromCreatePrError(error: unknown): string {
  const raw = extractCreatePrFailureMessage(error);
  const match = /No commits between [^ ]+ and ([^"\s]+)/i.exec(raw);
  return match?.[1] ? toStringTrim(match[1]).replace(/^refs\/heads\//, '') : '';
}

function isResourceAlreadyExistsDuringPrCreation(error: unknown): boolean {
  const msg = extractCreatePrFailureMessage(error);
  return /Resource ['"`][^'"`]+['"`] already exists at /i.test(msg);
}

async function registryResourceExistsOnDefaultBranch<
  ContextType extends ContextWithOctokit,
  ParamsType extends IssueParamsBase,
  TemplateType extends TemplateLikeBase,
>(
  context: ContextType,
  params: ParamsType,
  template: TemplateType,
  resourceName: string,
  callbacks: Pick<
    RequestPrCreationRecoveryCallbacks<ContextType, RepoInfoBase, IssueLikeBase, TemplateType, FormDataBase>,
    'getHttpStatus'
  >
): Promise<boolean> {
  const structRoot = resolveStructuredRootForTemplate(template);
  if (!structRoot || !resourceName) return false;

  for (const ext of ['yaml', 'yml']) {
    try {
      await context.octokit.repos.getContent({
        owner: params.owner,
        repo: params.repo,
        path: `${structRoot}/${resourceName}.${ext}`,
      });
      return true;
    } catch (error: unknown) {
      if (callbacks.getHttpStatus(error) === 404) continue;
      throw error;
    }
  }

  return false;
}

async function deleteBranchRefIfPresent<ContextType extends ContextWithOctokit>(
  context: ContextType,
  repoInfo: RepoInfoBase,
  branchName: string,
  callbacks: Pick<
    RequestPrCreationRecoveryCallbacks<ContextType, RepoInfoBase, IssueLikeBase, TemplateLikeBase, FormDataBase>,
    'getHttpStatus'
  >
): Promise<void> {
  const branch = toStringTrim(branchName).replace(/^refs\/heads\//, '');
  if (!branch) return;

  try {
    await context.octokit.git.deleteRef({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      ref: `heads/${branch}`,
    });
  } catch (error: unknown) {
    if (callbacks.getHttpStatus(error) !== 404) throw error;
  }
}

function formatCreateRequestFailureForUser(error: unknown, branchName = '', resourceName = ''): string {
  const msg = extractCreatePrFailureMessage(error);
  const parsedBranch = parseNoCommitsHeadBranchFromCreatePrError(error) || toStringTrim(branchName);

  if (/^No commits between\b/i.test(msg)) {
    const suffix = parsedBranch ? ` '${parsedBranch}'` : '';
    return `Failed to create PR automatically: stale request branch${suffix} blocked PR creation. Please retry approval.`;
  }

  if (isResourceAlreadyExistsDuringPrCreation(error)) {
    const suffix = resourceName ? ` '${resourceName}'` : '';
    return `Failed to create PR automatically: a stale request branch already contains${suffix}. Please retry approval.`;
  }

  return `Failed to create PR automatically: ${msg}`;
}

async function runCreateRequestPr<
  ContextType,
  RepoType extends RepoInfoBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
>(
  context: ContextType,
  repoInfo: RepoType,
  issue: IssueType,
  parsedFormData: FormDataType,
  template: TemplateType,
  callbacks: RequestPrCreationRecoveryCallbacks<ContextType, RepoType, IssueType, TemplateType, FormDataType>
): Promise<{ number: number }> {
  return await callbacks.createRequestPr(context, repoInfo, issue, parsedFormData, { template });
}

async function retryCreatePrAfterBranchCleanup<
  ContextType extends ContextWithOctokit,
  RepoType extends RepoInfoBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
>(
  context: ContextType,
  repoInfo: RepoType,
  branchName: string,
  issue: IssueType,
  parsedFormData: FormDataType,
  template: TemplateType,
  callbacks: RequestPrCreationRecoveryCallbacks<ContextType, RepoType, IssueType, TemplateType, FormDataType>
): Promise<{ number: number }> {
  await deleteBranchRefIfPresent(context, repoInfo, branchName, callbacks);
  return await runCreateRequestPr(context, repoInfo, issue, parsedFormData, template, callbacks);
}

async function handleNoCommitsCreatePrFailure<
  ContextType extends ContextWithOctokit,
  RepoType extends RepoInfoBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
>(
  context: ContextType,
  repoInfo: RepoType,
  branchName: string,
  issue: IssueType,
  parsedFormData: FormDataType,
  template: TemplateType,
  resourceName: string,
  callbacks: RequestPrCreationRecoveryCallbacks<ContextType, RepoType, IssueType, TemplateType, FormDataType>
): Promise<{ number: number }> {
  try {
    return await retryCreatePrAfterBranchCleanup(
      context,
      repoInfo,
      branchName,
      issue,
      parsedFormData,
      template,
      callbacks
    );
  } catch (retryError: unknown) {
    throw new Error(formatCreateRequestFailureForUser(retryError, branchName, resourceName), { cause: retryError });
  }
}

async function handleAlreadyExistsCreatePrFailure<
  ContextType extends ContextWithOctokit,
  RepoType extends RepoInfoBase,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
>(
  context: ContextType,
  args: {
    params: ParamsType;
    repoInfo: RepoType;
    issue: IssueType;
    parsedFormData: FormDataType;
    template: TemplateType;
    resourceName: string;
    branchName: string;
  },
  callbacks: RequestPrCreationRecoveryCallbacks<ContextType, RepoType, IssueType, TemplateType, FormDataType>
): Promise<{ number: number }> {
  const { params, repoInfo, issue, parsedFormData, template, resourceName, branchName } = args;

  try {
    const existsOnDefaultBranch = await registryResourceExistsOnDefaultBranch(
      context,
      params,
      template,
      resourceName,
      callbacks
    );

    if (existsOnDefaultBranch) {
      throw new Error(`Failed to create PR automatically: Resource '${resourceName}' already exists in the registry.`);
    }

    return await retryCreatePrAfterBranchCleanup(
      context,
      repoInfo,
      branchName,
      issue,
      parsedFormData,
      template,
      callbacks
    );
  } catch (retryError: unknown) {
    if (retryError instanceof Error && retryError.message.startsWith('Failed to create PR automatically:')) {
      throw retryError;
    }

    throw new Error(formatCreateRequestFailureForUser(retryError, branchName, resourceName), { cause: retryError });
  }
}

export async function createRequestPrWithRecovery<
  ContextType extends ContextWithOctokit,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  parsedFormData: FormDataType,
  template: TemplateType,
  resourceName: string,
  callbacks: RequestPrCreationRecoveryCallbacks<ContextType, RepoInfoBase, IssueType, TemplateType, FormDataType>
): Promise<{ number: number }> {
  const repoInfo: RepoInfoBase = { owner: params.owner, repo: params.repo };
  const fallbackBranchName = callbacks.renderConfiguredRequestBranchName(context, issue, resourceName);

  try {
    return await runCreateRequestPr(context, repoInfo, issue, parsedFormData, template, callbacks);
  } catch (error: unknown) {
    const staleNoCommitsBranch = parseNoCommitsHeadBranchFromCreatePrError(error) || fallbackBranchName;
    const failureMessage = extractCreatePrFailureMessage(error);

    if (/^No commits between\b/i.test(failureMessage)) {
      return await handleNoCommitsCreatePrFailure(
        context,
        repoInfo,
        staleNoCommitsBranch,
        issue,
        parsedFormData,
        template,
        resourceName,
        callbacks
      );
    }

    if (isResourceAlreadyExistsDuringPrCreation(error)) {
      return await handleAlreadyExistsCreatePrFailure(
        context,
        {
          params,
          repoInfo,
          issue,
          parsedFormData,
          template,
          resourceName,
          branchName: fallbackBranchName,
        },
        callbacks
      );
    }

    throw new Error(formatCreateRequestFailureForUser(error, staleNoCommitsBranch, resourceName), { cause: error });
  }
}
