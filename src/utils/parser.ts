type TemplateField = {
  id?: string;
  attributes?: {
    label?: string;
  };
};

type TemplateLike = {
  body?: TemplateField[];
};

const NO_RESPONSE_RE = /^_?\s*no\s*response\s*(?:provided)?\s*_?\.?$/i;

function normLabel(s: unknown): string {
  return String(s || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^\w]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function sanitizeScalar(v: unknown): string {
  const s = String(v ?? '').trim();
  if (!s) return '';
  if (NO_RESPONSE_RE.test(s)) return '';
  return s;
}

function extractValue(raw: string): string {
  const m = raw.match(/```[\w-]*\r?\n([\s\S]*?)\r?\n?```/);
  let v = m ? m[1] : raw;

  v = v.replace(/^\s+|\s+$/g, '');
  if (!m) v = v.replace(/\n{3,}/g, '\n\n');

  if (!v.trim()) return '';
  if (NO_RESPONSE_RE.test(v)) return '';

  v = v
    .replace(/^\s*>\s?/gm, '')
    .replace(/^\s*[-*]\s+\[[ xX]\]\s*/gm, '')
    .replace(/^\s*[-*]\s+/gm, '');

  v = v.replace(/^\s*[:-]\s*$/gm, '').trimEnd();

  return sanitizeScalar(v);
}

function buildFieldMap(template: TemplateLike): Record<string, string> {
  const map: Record<string, string> = {};

  for (const f of template?.body || []) {
    if (!f?.id) continue;

    const lbl = f?.attributes?.label || f.id;
    const idKey = normLabel(f.id);
    const lblKey = normLabel(lbl);

    map[idKey] = f.id;
    map[lblKey] = f.id;

    if (lblKey !== idKey) {
      map[`${lblKey}-${idKey}`] = f.id;
      map[`${idKey}-${lblKey}`] = f.id;
    }
  }

  return map;
}

function getMultilineValue(lines: string[], startIdx: number): { value: string; newIndex: number } {
  const buf: string[] = [];
  let j = startIdx;
  while (j < lines.length && lines[j] && !/^\s*[^:]+:\s*/.test(lines[j])) {
    buf.push(lines[j]);
    j++;
  }
  return { value: buf.join('\n'), newIndex: j - 1 };
}

function fallbackKvScan(text: string, fieldMap: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  const keys = Object.keys(fieldMap);
  if (!keys.length) return out;

  const lines = text.split(/\r?\n/);

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]?.trim();
    if (!line) continue;

    const m = /^([^:]+):\s*(.*)$/.exec(line);
    if (!m) continue;

    const keyNorm = normLabel(m[1]);
    const fieldId = fieldMap[keyNorm];
    if (!fieldId) continue;

    let value = m[2] || '';

    if (!value && i + 1 < lines.length && lines[i + 1] && !/^\s*[^:]+:\s*/.test(lines[i + 1])) {
      const { value: multiValue, newIndex } = getMultilineValue(lines, i + 1);
      i = newIndex;
      value = multiValue;
    }

    value = extractValue(value);
    if (value !== '') out[fieldId] = value;
  }

  return out;
}

export function parseForm(body: unknown, template: TemplateLike): Record<string, string> {
  const result: Record<string, string> = {};
  if (!body || !template) return result;

  const fieldMap = buildFieldMap(template);

  const text = String(body)
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/\r\n/g, '\n');

  const headingRe = /^#{2,6}[ \t]+([^\n]+?)\s*$/gm;

  const sections: { labelRaw: string; content: string }[] = [];
  const heads: { labelRaw: string; start: number; endOfLine: number }[] = [];

  let m: RegExpExecArray | null;
  while ((m = headingRe.exec(text)) !== null) {
    heads.push({ labelRaw: m[1], start: m.index, endOfLine: headingRe.lastIndex });
  }

  for (let i = 0; i < heads.length; i++) {
    const h = heads[i];
    const nextStart = i + 1 < heads.length ? heads[i + 1].start : text.length;
    const content = text.slice(h.endOfLine, nextStart);
    sections.push({ labelRaw: h.labelRaw, content });
  }

  for (const sec of sections) {
    const labelKey = normLabel(sec.labelRaw.replace(/:$/, ''));
    const fieldId = fieldMap[labelKey] || null;
    if (!fieldId) continue;

    const value = extractValue(sec.content);
    if (value === '') continue;

    result[fieldId] = value;
  }

  if (!Object.keys(result).length) {
    const kv = fallbackKvScan(text, fieldMap);
    for (const [k, v] of Object.entries(kv)) result[k] = v;
  }

  return result;
}
