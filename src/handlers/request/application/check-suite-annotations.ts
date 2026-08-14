import {
  collectRegistryValidationArtifacts,
  isRegistryValidateAnnotation,
  type CheckRunAnnotationLike,
  type RegistryValidationMachineReadableSource,
} from '../domain/registry-validation-annotations.js';

type CheckRunLikeBase = {
  id?: number | null;
};

export type CheckSuiteAnnotationsCallbacks<ContextType, CheckRunType extends CheckRunLikeBase> = {
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
  readCheckRunId: (run: CheckRunType | null) => number | null;
  listCheckRunsForSuite: (
    context: ContextType,
    args: {
      owner: string;
      repo: string;
      check_suite_id: number;
      per_page: number;
      page: number;
    }
  ) => Promise<{ data?: unknown }>;
  listCheckRunAnnotations: (
    context: ContextType,
    args: {
      owner: string;
      repo: string;
      check_run_id: number;
      per_page: number;
      page: number;
    }
  ) => Promise<{ data?: unknown }>;
  onCheckRunAnnotationsLoaded?: (
    context: ContextType,
    args: {
      checkRunId: number;
      annotationsTotal: number;
      relevant: number;
    }
  ) => void;
};

export type CheckSuiteRegistryValidationArtifacts = {
  runId: number;
  byFile: Map<string, string[]>;
  machineReadableSources: RegistryValidationMachineReadableSource[];
};

export async function listAllCheckRunsForSuite<ContextType, CheckRunType extends CheckRunLikeBase>(
  context: ContextType,
  owner: string,
  repo: string,
  checkSuiteId: number,
  callbacks: CheckSuiteAnnotationsCallbacks<ContextType, CheckRunType>
): Promise<CheckRunType[]> {
  const all: CheckRunType[] = [];
  let page = 1;

  while (true) {
    const res = await callbacks.listCheckRunsForSuite(context, {
      owner,
      repo,
      check_suite_id: checkSuiteId,
      per_page: 100,
      page,
    });

    const data = res.data;
    const runs =
      callbacks.isPlainObject(data) && Array.isArray(data['check_runs']) ? (data['check_runs'] as unknown[]) : [];

    all.push(...(runs as unknown as CheckRunType[]));

    if (runs.length < 100) break;
    page += 1;
    if (page > 20) break;
  }

  return all;
}

export async function listAllCheckRunAnnotations<
  ContextType,
  CheckRunType extends CheckRunLikeBase,
  CheckRunAnnotationType extends CheckRunAnnotationLike,
>(
  context: ContextType,
  owner: string,
  repo: string,
  checkRunId: number,
  callbacks: CheckSuiteAnnotationsCallbacks<ContextType, CheckRunType>
): Promise<CheckRunAnnotationType[]> {
  const all: CheckRunAnnotationType[] = [];
  let page = 1;

  while (true) {
    const res = await callbacks.listCheckRunAnnotations(context, {
      owner,
      repo,
      check_run_id: checkRunId,
      per_page: 100,
      page,
    });

    const data = res.data;
    const items = Array.isArray(data) ? (data as unknown[]) : [];

    all.push(...(items as unknown as CheckRunAnnotationType[]));

    if (items.length < 100) break;
    page += 1;
    if (page > 20) break;
  }

  return all;
}

export async function readFirstRegistryValidationArtifactsForSuiteRuns<
  ContextType,
  CheckRunType extends CheckRunLikeBase,
  CheckRunAnnotationType extends CheckRunAnnotationLike,
>(
  context: ContextType,
  owner: string,
  repo: string,
  runsForSuite: CheckRunType[],
  callbacks: CheckSuiteAnnotationsCallbacks<ContextType, CheckRunType>
): Promise<CheckSuiteRegistryValidationArtifacts | null> {
  for (const run of runsForSuite) {
    const runId = callbacks.readCheckRunId(run);
    if (!runId) continue;

    let annotations: CheckRunAnnotationType[];
    try {
      annotations = await listAllCheckRunAnnotations(context, owner, repo, runId, callbacks);
    } catch {
      continue;
    }

    const relevant = annotations.filter(isRegistryValidateAnnotation);

    callbacks.onCheckRunAnnotationsLoaded?.(context, {
      checkRunId: runId,
      annotationsTotal: annotations.length,
      relevant: relevant.length,
    });

    if (!relevant.length) continue;

    const { byFile, machineReadableSources } = collectRegistryValidationArtifacts(relevant);
    return { runId, byFile, machineReadableSources };
  }

  return null;
}
