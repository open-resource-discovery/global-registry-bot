import { toStringTrim as toStringTrimPure } from '../domain/login-utils.js';
import { isPlainObject } from './errors.js';

export type RepoInfo = {
  owner: string;
  repo: string;
};

function toStringTrim(value: unknown): string {
  return toStringTrimPure(value);
}

function normalizeRepoPath(path: unknown): string {
  return toStringTrim(path)
    .replace(/\\/g, '/')
    .replace(/^\/+/, '')
    .replace(/\/{2,}/g, '/');
}

export function readRepoInfoFromPayload(payload: unknown): RepoInfo | null {
  if (!isPlainObject(payload)) return null;

  const repoObj = payload['repository'];
  if (!isPlainObject(repoObj)) return null;

  const repoName = toStringTrim(repoObj['name']);
  const ownerObj = isPlainObject(repoObj['owner']) ? repoObj['owner'] : null;
  const ownerLogin = ownerObj ? toStringTrim(ownerObj['login']) : '';

  if (!ownerLogin || !repoName) return null;

  return { owner: ownerLogin, repo: repoName };
}

export function readDefaultBranchFromPayload(payload: unknown): string {
  if (!isPlainObject(payload)) return '';

  const repoObj = isPlainObject(payload['repository']) ? payload['repository'] : null;
  return repoObj ? toStringTrim(repoObj['default_branch']) : '';
}

export function readDefaultBranchFromPush(payload: unknown): string {
  return readDefaultBranchFromPayload(payload);
}

export function readPushChangedFiles(payload: unknown): string[] {
  if (!isPlainObject(payload)) return [];

  const commits = Array.isArray(payload['commits']) ? payload['commits'] : [];
  const out: string[] = [];
  const seen = new Set<string>();

  for (const commit of commits) {
    if (!isPlainObject(commit)) continue;

    for (const key of ['added', 'modified', 'removed'] as const) {
      const files = Array.isArray(commit[key]) ? commit[key] : [];
      for (const file of files) {
        const normalized = normalizeRepoPath(file);
        if (!normalized || seen.has(normalized)) continue;
        seen.add(normalized);
        out.push(normalized);
      }
    }
  }

  return out;
}

export function readPayloadLabelName(payload: unknown): string {
  if (!isPlainObject(payload)) return '';
  const l = payload['label'];
  if (typeof l === 'string') return toStringTrim(l);
  if (isPlainObject(l)) return toStringTrim(l['name']);
  return '';
}
