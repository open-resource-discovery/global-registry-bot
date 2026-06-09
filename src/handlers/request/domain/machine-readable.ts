export type MachineReadableIssue = Readonly<{
  field: string;
  message: string;
  filePath?: string;
}>;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

export function normalizeMachineReadableIssues(value: unknown): MachineReadableIssue[] {
  const items = Array.isArray(value) ? value : [];
  const out: MachineReadableIssue[] = [];
  const seen = new Set<string>();

  for (const item of items) {
    if (!isPlainObject(item)) continue;

    const message = toStringTrim(item['message']);
    const field = toStringTrim(item['field'] ?? item['path']) || 'details';
    const filePath = toStringTrim(item['filePath']);

    if (!message) continue;

    const key = `${field}\u0000${filePath}\u0000${message}`;
    if (seen.has(key)) continue;
    seen.add(key);

    out.push({
      field,
      message,
      ...(filePath ? { filePath } : {}),
    });
  }

  return out;
}

export function buildMachineReadableMetadataBlock(issues: MachineReadableIssue[]): string {
  const normalized = normalizeMachineReadableIssues(issues);
  if (!normalized.length) return '';

  return `
##
<details>
<summary>Show as JSON (Robots Friendly)</summary>

\`\`\`json
${JSON.stringify(normalized, null, 2)}
\`\`\`
</details>`;
}

export function buildDetectedIssuesBody(message: string, issues: MachineReadableIssue[] = []): string {
  return `## Detected issues

${message}${buildMachineReadableMetadataBlock(issues)}`;
}

export function singleMachineReadableIssue(field: string, message: string, filePath = ''): MachineReadableIssue[] {
  const normalizedMessage = toStringTrim(message);
  const normalizedField = toStringTrim(field) || 'details';
  const normalizedFilePath = toStringTrim(filePath);

  return normalizedMessage
    ? [
        {
          field: normalizedField,
          message: normalizedMessage,
          ...(normalizedFilePath ? { filePath: normalizedFilePath } : {}),
        },
      ]
    : [];
}
