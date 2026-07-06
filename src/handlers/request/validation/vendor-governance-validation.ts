type RequestConfigEntry = {
  folderName?: string;
  [k: string]: unknown;
};

type ValidationContext = {
  octokit: {
    rest: {
      repos: {
        getContent: (args: { owner: string; repo: string; path: string }) => Promise<unknown>;
      };
    };
  };
};

function isNamespaceLikeRequestType(requestType: unknown, toStringSafe: (value: unknown) => string): boolean {
  const rt = toStringSafe(requestType)
    .replace(/\s|_|-/g, '')
    .toLowerCase();
  if (!rt || rt === 'vendor') return false;

  return rt.includes('namespace') || rt === 'system' || rt === 'subcontext' || rt === 'authority';
}

function isSystemNamespaceRequestType(requestType: unknown, toStringSafe: (value: unknown) => string): boolean {
  const rt = toStringSafe(requestType)
    .replace(/\s|_|-/g, '')
    .toLowerCase();
  return rt === 'systemnamespace' || rt === 'system';
}

function extractVendorRootFromResourceName(resourceName: unknown, toStringSafe: (value: unknown) => string): string {
  const raw = toStringSafe(resourceName).replaceAll('\u00a0', ' ').trim();
  if (!raw) return '';

  const first = raw
    .split('.')
    .map((part) => part.trim())
    .filter(Boolean)[0];

  return toStringSafe(first).toLowerCase();
}

function normalizeStringArray(value: unknown, toStringSafe: (value: unknown) => string): string[] {
  const raw = Array.isArray(value) ? value : value !== undefined && value !== null ? [value] : [];

  return Array.from(
    new Set(raw.map((entry) => toStringSafe(entry).replace(/^@+/, '').trim().toLowerCase()).filter(Boolean))
  );
}

function resolveVendorRegistryRoot(args: {
  getVendorRequestConfig: () => RequestConfigEntry | null;
  toStringSafe: (value: unknown) => string;
}): string {
  const vendorCfg = args.getVendorRequestConfig();
  const folder = args.toStringSafe(vendorCfg?.folderName).replace(/^\/+/, '').replace(/\/+$/, '');
  return folder || 'data/vendors';
}

function resolveAllowedSystemNamespaceVendors(
  requestCfg: RequestConfigEntry | null,
  toStringSafe: (value: unknown) => string
): string[] {
  const configured = normalizeStringArray(requestCfg?.['allowedVendorRoots'], toStringSafe);
  if (configured.length) return configured;

  const legacy = normalizeStringArray(requestCfg?.['allowedVendors'], toStringSafe);
  if (legacy.length) return legacy;

  return ['sap'];
}

async function repoPathExists(args: {
  context: ValidationContext;
  owner: string;
  repo: string;
  repoPath: string;
  getHttpStatus: (error: unknown) => number | undefined;
}): Promise<boolean> {
  try {
    await args.context.octokit.rest.repos.getContent({ owner: args.owner, repo: args.repo, path: args.repoPath });
    return true;
  } catch (error: unknown) {
    if (args.getHttpStatus(error) === 404) return false;
    throw error;
  }
}

export async function collectVendorGovernanceErrors(args: {
  context: ValidationContext;
  owner: string;
  repo: string;
  requestType: string;
  requestCfg: RequestConfigEntry | null;
  resourceName: string;
  getVendorRequestConfig: () => RequestConfigEntry | null;
  getHttpStatus: (error: unknown) => number | undefined;
  toStringSafe: (value: unknown) => string;
}): Promise<string[]> {
  if (!isNamespaceLikeRequestType(args.requestType, args.toStringSafe)) return [];

  const vendorRoot = extractVendorRootFromResourceName(args.resourceName, args.toStringSafe);
  if (!vendorRoot) return [];

  const vendorRegistryRoot = resolveVendorRegistryRoot({
    getVendorRequestConfig: args.getVendorRequestConfig,
    toStringSafe: args.toStringSafe,
  });
  const vendorYamlPath = `${vendorRegistryRoot}/${vendorRoot}.yaml`;
  const vendorYmlPath = `${vendorRegistryRoot}/${vendorRoot}.yml`;

  const hasVendorEntry =
    (await repoPathExists({
      context: args.context,
      owner: args.owner,
      repo: args.repo,
      repoPath: vendorYamlPath,
      getHttpStatus: args.getHttpStatus,
    })) ||
    (await repoPathExists({
      context: args.context,
      owner: args.owner,
      repo: args.repo,
      repoPath: vendorYmlPath,
      getHttpStatus: args.getHttpStatus,
    }));

  const errors: string[] = [];

  if (!hasVendorEntry) {
    errors.push(
      `Vendor '${vendorRoot}' is not registered. Please register '${vendorRoot}' first before requesting '${args.resourceName}'.`
    );
  }

  if (isSystemNamespaceRequestType(args.requestType, args.toStringSafe)) {
    const allowedVendorRoots = resolveAllowedSystemNamespaceVendors(args.requestCfg, args.toStringSafe);

    if (!allowedVendorRoots.includes(vendorRoot)) {
      errors.push(
        `System namespaces are only allowed for vendor roots: ${allowedVendorRoots.join(', ')}. Requested vendor root: '${vendorRoot}'.`
      );
    }
  }

  return errors;
}
