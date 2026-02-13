/* eslint-disable no-console */
import YAML from 'yaml';
import { readFile, appendFile } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const CONFIG_BASE_DIR = '.github/registry-bot';

type Mode = 'pr' | 'main';

type RequestEntry = {
  folderName: string;
  schema: string;
  approvers?: string[] | null;
};

type LoadedConfig = {
  requests: Record<string, RequestEntry>;
  defaultApprovers: string[];
};

type SchemaCandidate = { requestType: string; folder: string };

type SenderLike = { type?: string; login?: string };

const isBotSender = (sender: SenderLike | undefined | null): boolean =>
  sender?.type === 'Bot' || /(\[bot\]|-bot)$/i.test(sender?.login || '');

const execFileAsync = promisify(execFile);

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function normalizeStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((x) => String(x ?? '').trim()).filter(Boolean);
}

function normalizeTypeToken(v: unknown): string {
  if (typeof v !== 'string') return '';
  return v.trim().toLowerCase();
}

function normalizeKey(v: unknown): string {
  return String(v ?? '')
    .trim()
    .toLowerCase();
}

function readRegistryValidateOkEnv(): boolean | null {
  const raw = normalizeKey(process.env.REGISTRY_VALIDATE_OK ?? '');
  if (!raw) return null;

  if (raw === 'true' || raw === '1' || raw === 'yes' || raw === 'success' || raw === 'passed') {
    return true;
  }
  if (raw === 'false' || raw === '0' || raw === 'no' || raw === 'failure' || raw === 'failed') {
    return false;
  }

  return null;
}

function normalizeRepoPath(p: string): string {
  const s = String(p ?? '')
    .trim()
    .replace(/\\/g, '/')
    .replace(/\/{2,}/g, '/');

  return s
    .replace(/^(\.\/)+/, '')
    .replace(/^\/+/, '')
    .replace(/\/+$/, '');
}

function isYamlPath(p: string): boolean {
  const s = p.toLowerCase();
  return s.endsWith('.yaml') || s.endsWith('.yml');
}

function pickMode(): Mode {
  const forced = String(process.env.REGISTRY_VALIDATE_MODE ?? '')
    .trim()
    .toLowerCase();
  if (forced === 'pr' || forced === 'main') return forced;

  const eventName = String(process.env.GITHUB_EVENT_NAME ?? '').trim();
  return eventName === 'pull_request' ? 'pr' : 'main';
}

async function loadConfig(): Promise<LoadedConfig> {
  const paths = [`${CONFIG_BASE_DIR}/config.yaml`, `${CONFIG_BASE_DIR}/config.yml`];

  let raw: string | null = null;
  let usedPath: string | null = null;

  for (const p of paths) {
    try {
      raw = await readFile(p, 'utf8');
      usedPath = p;
      break;
    } catch {
      // ignore
    }
  }

  if (!raw || !usedPath) {
    throw new Error(`Missing registry-bot config: expected ${CONFIG_BASE_DIR}/config.yaml or .yml`);
  }

  const parsed: unknown = YAML.parse(raw);
  if (!isPlainObject(parsed)) throw new Error(`Invalid YAML in ${usedPath}`);

  const out: LoadedConfig = { requests: {}, defaultApprovers: [] };

  const workflowRaw = parsed['workflow'];
  if (isPlainObject(workflowRaw)) {
    const ap = workflowRaw['approvers'];
    if (ap !== undefined && ap !== null) {
      out.defaultApprovers = normalizeStringArray(ap);
    }
  }

  const requestsRaw = parsed['requests'];
  if (!isPlainObject(requestsRaw)) return out;

  for (const [requestType, rc0] of Object.entries(requestsRaw)) {
    if (!isPlainObject(rc0)) continue;

    const folderName = rc0['folderName'];
    const schema = rc0['schema'];

    if (typeof folderName !== 'string' || typeof schema !== 'string') continue;

    const folder = normalizeRepoPath(folderName);
    let schemaPath = normalizeRepoPath(schema);

    if (schemaPath && !schemaPath.startsWith(`${CONFIG_BASE_DIR}/`) && !schemaPath.startsWith('.github/')) {
      schemaPath = `${CONFIG_BASE_DIR}/${schemaPath}`;
    }

    const approversRaw = rc0['approvers'];
    let approvers: string[] | null | undefined = undefined;

    if (approversRaw === null) approvers = null;
    else if (approversRaw !== undefined) {
      const arr = normalizeStringArray(approversRaw);
      if (arr.length > 0) approvers = arr;
    }

    out.requests[requestType] = { folderName: folder, schema: schemaPath, approvers };
  }

  return out;
}

function matchRequestTypesForFile(filePath: string, requests: Record<string, RequestEntry>): SchemaCandidate[] {
  const fp = normalizeRepoPath(filePath);
  const candidates: SchemaCandidate[] = [];

  for (const [requestType, cfg] of Object.entries(requests)) {
    const folder = normalizeRepoPath(cfg.folderName);
    if (!folder) continue;

    if (fp === folder || fp.startsWith(`${folder}/`)) {
      candidates.push({ requestType, folder });
    }
  }

  return candidates;
}

async function getChangedFiles(baseSha: string, headSha: string): Promise<string[]> {
  const { stdout } = await execFileAsync('git', ['diff', '--name-only', '--diff-filter=AMR', baseSha, headSha], {
    maxBuffer: 5 * 1024 * 1024,
  });

  return String(stdout ?? '')
    .split('\n')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => s.replace(/\\/g, '/'));
}

async function readDocType(path: string): Promise<string> {
  const fileFsPath = normalizeRepoPath(path);
  try {
    const raw = await readFile(fileFsPath, 'utf8');
    const doc: unknown = YAML.parse(raw);
    if (!isPlainObject(doc)) return '';
    return normalizeTypeToken(doc['type']);
  } catch {
    return '';
  }
}

async function pickRequestTypeForFile(filePath: string, candidates: SchemaCandidate[]): Promise<string> {
  if (candidates.length === 0) return '';
  if (candidates.length === 1) return candidates[0].requestType;

  const docType = await readDocType(filePath);
  if (!docType) return '';

  const match = candidates.find((c) => normalizeKey(c.requestType) === normalizeKey(docType));
  return match?.requestType ?? '';
}

type OutputPayload = {
  reviewers: string[];
  senderIsBot: boolean;
  prCreatorIsBot: boolean;
  prHumanTouched: boolean;
  botLogin: string;
};

async function writeOutputs(payload: OutputPayload): Promise<void> {
  const outPath = String(process.env.GITHUB_OUTPUT ?? '').trim();
  if (!outPath) return;

  const csv = payload.reviewers.join(',');
  const json = JSON.stringify(payload.reviewers);

  const lines = [
    `reviewers=${csv}`,
    `reviewers_json=${json}`,
    `reviewers_count=${payload.reviewers.length}`,
    `sender_is_bot=${payload.senderIsBot ? 'true' : 'false'}`,
    `pr_creator_is_bot=${payload.prCreatorIsBot ? 'true' : 'false'}`,
    `bot_login=${payload.botLogin}`,
    `pr_human_touched=${payload.prHumanTouched ? 'true' : 'false'}`,
  ];

  await appendFile(outPath, `${lines.join('\n')}\n`, 'utf8');
}

async function main(): Promise<void> {
  const mode = pickMode();
  const eventName = String(process.env.GITHUB_EVENT_NAME ?? '').trim();
  const eventAction = normalizeKey(process.env.PR_EVENT_ACTION ?? '');

  const sender: SenderLike = {
    type: String(process.env.PR_SENDER_TYPE ?? '').trim(),
    login: String(process.env.PR_SENDER_LOGIN ?? '').trim(),
  };

  const creator: SenderLike = {
    type: String(process.env.PR_CREATOR_TYPE ?? '').trim(),
    login: String(process.env.PR_CREATOR_LOGIN ?? '').trim(),
  };

  const senderIsBot = isBotSender(sender);
  const prCreatorIsBot = isBotSender(creator);
  const botLogin = (prCreatorIsBot && creator.login ? creator.login : '').trim();

  const prHumanTouched =
    String(process.env.PR_HUMAN_TOUCHED ?? '')
      .trim()
      .toLowerCase() === 'true';

  const baseOut: Omit<OutputPayload, 'reviewers'> = {
    senderIsBot,
    prCreatorIsBot,
    prHumanTouched,
    botLogin,
  };

  if (mode !== 'pr' || eventName !== 'pull_request') {
    await writeOutputs({ reviewers: [], ...baseOut });
    return;
  }

  const isFork =
    String(process.env.PR_IS_FORK ?? '')
      .trim()
      .toLowerCase() === 'true';
  if (isFork) {
    await writeOutputs({ reviewers: [], ...baseOut });
    return;
  }

  const baseSha = String(process.env.PR_BASE_SHA ?? '').trim();
  const headSha = String(process.env.PR_HEAD_SHA ?? '').trim();
  if (!baseSha || !headSha) {
    await writeOutputs({ reviewers: [], ...baseOut });
    return;
  }

  const botCreatedPr = prCreatorIsBot && Boolean(botLogin);
  // Fallback for environments that don't provide PR_HUMAN_TOUCHED.
  const humanSyncOnBotPr = botCreatedPr && !senderIsBot && eventAction === 'synchronize';
  const effectiveHumanTouched = prHumanTouched || humanSyncOnBotPr;

  // Bot-created PRs should NOT request config approvers.
  // Only switch to config approvers if a human actually pushed commits
  if (botCreatedPr && !effectiveHumanTouched) {
    const reviewers: string[] = [];
    console.log('Computed PR reviewers: (none)');
    await writeOutputs({ reviewers, ...baseOut });
    return;
  }

  // Gate reviewer requests on registry validation
  const registryValidateOk = readRegistryValidateOkEnv();
  if (registryValidateOk === false) {
    const reviewers: string[] = [];
    console.log('Computed PR reviewers: (none)');
    await writeOutputs({ reviewers, ...baseOut });
    return;
  }

  const prAuthor = normalizeKey(process.env.PR_AUTHOR ?? '');

  const cfg = await loadConfig();

  const changed = await getChangedFiles(baseSha, headSha);
  const yamlFiles = changed.filter(isYamlPath);

  const requestTypes = new Set<string>();
  let needsDefault = false;

  for (const p of yamlFiles) {
    const candidates = matchRequestTypesForFile(p, cfg.requests);
    if (candidates.length === 0) continue;

    const rt = await pickRequestTypeForFile(p, candidates);
    if (rt) requestTypes.add(rt);
    else needsDefault = true;
  }

  const reviewersSet = new Set<string>();

  const addReviewers = (arr: string[]): void => {
    for (const r of arr) {
      const trimmed = r.trim();
      const key = normalizeKey(trimmed);
      if (!key) continue;
      if (isBotSender({ login: trimmed })) continue;
      if (prAuthor && key === prAuthor) continue;
      reviewersSet.add(trimmed);
    }
  };

  if (needsDefault && cfg.defaultApprovers.length > 0) addReviewers(cfg.defaultApprovers);

  for (const rt of requestTypes) {
    const entry = cfg.requests[rt];
    const override = entry?.approvers;

    if (Array.isArray(override) && override.length > 0) addReviewers(override);
    else if (cfg.defaultApprovers.length > 0) addReviewers(cfg.defaultApprovers);
  }

  const reviewers = Array.from(reviewersSet)
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));

  console.log(`Computed PR reviewers: ${reviewers.join(', ') || '(none)'}`);
  await writeOutputs({ reviewers, ...baseOut });
}

export const TEST_UTILS = {
  normalizeRepoPath,
  matchRequestTypesForFile,
  isBotSender,
};

export { main };

// Prevent auto-run when imported by Jest
if (!process.env.JEST_WORKER_ID) {
  main().catch((e: unknown) => {
    const msg = e instanceof Error ? e.message : String(e);
    console.error(msg);
    process.exitCode = 1;
  });
}
