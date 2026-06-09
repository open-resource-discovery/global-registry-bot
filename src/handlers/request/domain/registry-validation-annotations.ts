export type CheckRunAnnotationLike = {
  path?: string | null;
  message?: string | null;
  title?: string | null;
  annotation_level?: string | null;
  raw_details?: string | null;
};

export type RegistryValidationMachineReadableSource = Readonly<{
  filePath: string;
  message: string;
  schemaPath?: string;
}>;

function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

const normalizeKey = (value: unknown): string => {
  const base = toStringTrim(value).toLowerCase();
  return base.replaceAll(/[^\w]+/g, '-').replaceAll(/(?:^-+|-+$)/g, '');
};

export function isRegistryValidateAnnotation(annotation: CheckRunAnnotationLike): boolean {
  const title = toStringTrim(annotation?.title).toLowerCase();
  return title.startsWith('registry-validate');
}

export function stripRegistrySuffix(message: string): string {
  const index = message.indexOf(' [file=');
  return (index >= 0 ? message.slice(0, index) : message).trim();
}

export function toSectionTitle(field: string): string {
  const raw = toStringTrim(field);
  if (!raw) return 'Details';

  const lowerCase = raw.toLowerCase();
  if (lowerCase === 'contact' || lowerCase === 'contacts') return 'Contacts';

  const spaced = raw
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .trim();

  if (!spaced) return 'Details';
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

export function normalizeMsg(message: string): string {
  const text = toStringTrim(message);

  const firstSpace = text.indexOf(' ');
  const maybePath = firstSpace > 0 ? text.slice(0, firstSpace) : '';
  const rest = firstSpace > 0 ? text.slice(firstSpace + 1).trim() : text;
  const msgOnly = maybePath.startsWith('/') ? rest : text;

  return msgOnly.replace(/\bmust\b/gi, 'MUST');
}

export function extractFieldFromMsg(message: string): string {
  const text = toStringTrim(message);
  if (!text) return '';

  const pointer = /^\/([^/\s]+)(?:\/|\s|$)/.exec(text);
  if (pointer?.[1]) return pointer[1];

  const requiredProperty = /\b(?:required property|Property)\s*['"]([^'"]+)['"]/.exec(text);
  if (requiredProperty?.[1]) return requiredProperty[1];

  const additionalProperty = /\badditional property\s*['"]([^'"]+)['"]/.exec(text);
  if (additionalProperty?.[1]) return additionalProperty[1];

  const labelRequired = /^(.+?)\s+is\s+required\.\s*$/i.exec(text);
  if (labelRequired?.[1]) return normalizeKey(labelRequired[1]);

  const leadingField = /^([a-z][a-zA-Z0-9_-]*)\s+(?:must|MUST)\b/.exec(text);
  if (leadingField?.[1]) return leadingField[1];

  const dotted = /^([a-z][a-zA-Z0-9_-]*)(?:\[[^\]]*\])?\.[a-zA-Z0-9_-]+\s+is\s+required\./i.exec(text);
  if (dotted?.[1]) return dotted[1];

  return '';
}

function groupRegistryValidationMessages(messages: string[]): Map<string, string[]> {
  const grouped = new Map<string, string[]>();

  for (const raw of messages) {
    const field = extractFieldFromMsg(raw) || 'details';
    const msg = normalizeMsg(raw);
    if (!msg) continue;

    const items = grouped.get(field) ?? [];
    if (!items.includes(msg)) items.push(msg);
    grouped.set(field, items);
  }

  return grouped;
}

function sortRegistryValidationGroupKeys(grouped: Map<string, string[]>): string[] {
  return Array.from(grouped.keys()).sort((left, right) => {
    if (left === 'details') return 1;
    if (right === 'details') return -1;
    return left.localeCompare(right);
  });
}

function appendRegistryValidationSections(lines: string[], grouped: Map<string, string[]>, headingLevel: string): void {
  for (const key of sortRegistryValidationGroupKeys(grouped)) {
    lines.push(`${headingLevel} ${toSectionTitle(key)}`);
    for (const msg of grouped.get(key) ?? []) {
      lines.push(`- ${msg}`);
    }
    lines.push('');
  }
}

function appendRegistryValidationFileSection(lines: string[], filePath: string, messages: string[]): void {
  lines.push(`### File: \`${filePath}\``, '');
  appendRegistryValidationSections(lines, groupRegistryValidationMessages(messages), '####');
}

export function filterRegistryValidationEntries(byFile: Map<string, string[]>): [string, string[]][] {
  return Array.from(byFile.entries())
    .filter(([, messages]) => Array.isArray(messages) && messages.length > 0)
    .sort(([left], [right]) => left.localeCompare(right));
}

export function filterMachineReadableSourcesForFile(
  machineReadableSources: RegistryValidationMachineReadableSource[],
  filePath: string
): RegistryValidationMachineReadableSource[] {
  return machineReadableSources.filter((item) => toStringTrim(item.filePath) === toStringTrim(filePath));
}

export function buildRegistryValidationCommentHeading(
  filePath: string,
  messages: string[],
  headingLevel: '###' | '####' = '###'
): string[] {
  const lines: string[] = [];

  if (headingLevel === '###') {
    lines.push(`### File: \`${filePath}\``, '');
    appendRegistryValidationSections(lines, groupRegistryValidationMessages(messages), '###');
    return lines;
  }

  appendRegistryValidationFileSection(lines, filePath, messages);
  return lines;
}

export function buildRegistryValidationAggregateBody(byFile: Map<string, string[]>): string {
  const entries = filterRegistryValidationEntries(byFile);
  if (!entries.length) return '';

  const lines: string[] = ['## Detected issues', ''];
  for (const [filePath, messages] of entries) {
    appendRegistryValidationFileSection(lines, filePath, messages);
  }

  return lines.join('\n').trimEnd();
}

export function collectRegistryValidationArtifacts(annotations: CheckRunAnnotationLike[]): {
  byFile: Map<string, string[]>;
  machineReadableSources: RegistryValidationMachineReadableSource[];
} {
  const byFile = new Map<string, string[]>();
  const machineReadableSources: RegistryValidationMachineReadableSource[] = [];

  for (const annotation of annotations) {
    const file = toStringTrim(annotation.path) || 'unknown file';
    const rawMsg = toStringTrim(annotation.message) || toStringTrim(annotation.raw_details);
    const msg = stripRegistrySuffix(rawMsg);
    if (!msg) continue;

    const schemaMeta = /\bschema=([^\s\]]+)/.exec(rawMsg) ?? /\[schema=([^\]]+)\]/.exec(rawMsg);
    const messages = byFile.get(file) ?? [];
    messages.push(msg);
    byFile.set(file, messages);

    machineReadableSources.push({
      filePath: file,
      message: msg,
      schemaPath: schemaMeta?.[1] ? toStringTrim(schemaMeta[1]) : '',
    });
  }

  return { byFile, machineReadableSources };
}
