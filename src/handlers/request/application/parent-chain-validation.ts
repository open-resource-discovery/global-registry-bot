import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type TemplateLikeBase = {
  _meta?: {
    root?: unknown;
    requestType?: unknown;
  };
};

export type ParentChainValidationCallbacks<
  ContextType,
  TemplateLikeType extends TemplateLikeBase,
  FormDataType extends Record<string, string>,
> = {
  extractResourceNameFromForm: (formData: FormDataType, template: TemplateLikeType) => string;
  resolveVendorRegistryRoot: (context: ContextType) => string;
  getHttpStatus: (error: unknown) => number | undefined;
};

export async function repoYamlExistsApplication<ContextType>(
  context: ContextType,
  repo: RepoInfoBase,
  basePath: string,
  getHttpStatus: (error: unknown) => number | undefined
): Promise<boolean> {
  for (const ext of ['yaml', 'yml']) {
    try {
      await (
        context as unknown as {
          octokit: {
            repos: {
              getContent: (args: { owner: string; repo: string; path: string }) => Promise<unknown>;
            };
          };
        }
      ).octokit.repos.getContent({
        owner: repo.owner,
        repo: repo.repo,
        path: `${basePath}.${ext}`,
      });
      return true;
    } catch (e: unknown) {
      if (getHttpStatus(e) !== 404) throw e;
    }
  }

  return false;
}

export async function checkParentChainExistsInFlatStructureApplication<
  ContextType,
  TemplateLikeType extends TemplateLikeBase,
  FormDataType extends Record<string, string>,
>(
  context: ContextType,
  repoInfo: RepoInfoBase,
  template: TemplateLikeType,
  formData: FormDataType,
  callbacks: ParentChainValidationCallbacks<ContextType, TemplateLikeType, FormDataType>,
  explicitResourceName?: string
): Promise<string | null> {
  const rootRaw = toStringTrim(template?._meta?.root);
  const structRoot = rootRaw.replace(/^\/+/, '').replace(/\/+$/, '');
  if (!structRoot) return null;

  const rt = toStringTrim(template?._meta?.requestType).toLowerCase();
  const isNamespaceLike = rt.includes('namespace') || rt === 'subcontext' || rt === 'system' || rt === 'authority';
  if (!isNamespaceLike) return null;

  const resourceName = toStringTrim(explicitResourceName) || callbacks.extractResourceNameFromForm(formData, template);
  const parts = toStringTrim(resourceName).split('.').filter(Boolean);
  if (parts.length < 2) return null;

  const vendorRoot = callbacks.resolveVendorRegistryRoot(context);

  for (let i = parts.length - 1; i >= 1; i -= 1) {
    const parentName = parts.slice(0, i).join('.');
    if (!parentName) continue;

    const exists =
      i === 1
        ? await repoYamlExistsApplication(context, repoInfo, `${vendorRoot}/${parentName}`, callbacks.getHttpStatus)
        : await repoYamlExistsApplication(context, repoInfo, `${structRoot}/${parentName}`, callbacks.getHttpStatus);

    if (exists) continue;

    return i === 1
      ? `Vendor '${parentName}' is not present. Please register the vendor first.`
      : `Parent resource '${parentName}' is not present. Please register the parent first.`;
  }

  return null;
}
