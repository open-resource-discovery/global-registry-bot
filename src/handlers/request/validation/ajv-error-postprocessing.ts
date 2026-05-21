export type AjvErrorLike = {
  keyword?: string;
  instancePath?: string;
  schemaPath?: string;
  message?: string;
  params?: Record<string, unknown>;
};

type PostprocessingHelpers = {
  toStringSafe: (v: unknown) => string;
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
};

export function dedupe(arr: unknown): string[] {
  const a = Array.isArray(arr) ? arr : [];
  return Array.from(new Set(a.map((s) => String(s).trim()).filter(Boolean)));
}

export function getValueAtInstancePath(
  obj: unknown,
  instancePath: unknown,
  helpers: Pick<PostprocessingHelpers, 'toStringSafe' | 'isPlainObject'>
): unknown {
  const p = helpers.toStringSafe(instancePath);
  if (!p || p === '/') return obj;

  const parts = p.split('/').filter(Boolean);
  let cur: unknown = obj;

  for (const part of parts) {
    if (cur === null || cur === undefined) return undefined;

    if (Array.isArray(cur) && /^\d+$/.test(part)) {
      cur = cur[Number(part)];
      continue;
    }

    if (helpers.isPlainObject(cur)) {
      cur = cur[part];
      continue;
    }

    return undefined;
  }

  return cur;
}

export function filterNoisyOneOfTypeErrors(
  ajvErrs: unknown,
  candidate: unknown,
  helpers: Pick<PostprocessingHelpers, 'toStringSafe' | 'isPlainObject'>
): AjvErrorLike[] {
  const errs = Array.isArray(ajvErrs) ? (ajvErrs as unknown[]) : [];

  const hasSpecificErrorAtPath = new Set(
    errs
      .filter((e) => helpers.isPlainObject(e))
      .map((e) => e as AjvErrorLike)
      .filter(
        (e) =>
          String(e.instancePath || '') &&
          ['pattern', 'format', 'minItems', 'uniqueItems', 'errorMessage', 'oneOf', 'anyOf'].includes(
            String(e.keyword || '')
          )
      )
      .map((e) => String(e.instancePath || ''))
  );

  const sane: AjvErrorLike[] = errs.filter(helpers.isPlainObject).map((e) => e as AjvErrorLike);
  return sane.filter((e) => {
    if (e.keyword === 'type' && helpers.toStringSafe(e.params?.['type']) === 'string') {
      const path = String(e.instancePath || '');
      const val = getValueAtInstancePath(candidate, path, helpers);

      if (Array.isArray(val) && hasSpecificErrorAtPath.has(path)) {
        return false;
      }
    }

    return true;
  });
}

export function normalizeAjvMessage(msg: unknown, helpers: Pick<PostprocessingHelpers, 'toStringSafe'>): string {
  const raw = helpers.toStringSafe(msg);
  if (!raw) return '';

  let out = raw
    .replaceAll(/\bmust\s+not\b/gi, 'MUST NOT')
    .replaceAll(/\bshall\s+not\b/gi, 'SHALL NOT')
    .replaceAll(/\bshould\s+not\b/gi, 'SHOULD NOT')
    .replaceAll(/\bmust\b/gi, 'MUST')
    .replaceAll(/\brequired\b/gi, 'REQUIRED')
    .replaceAll(/\bshall\b/gi, 'SHALL')
    .replaceAll(/\bshould\b/gi, 'SHOULD')
    .replaceAll(/\brecommended\b/gi, 'RECOMMENDED')
    .replaceAll(/\bmay\b/gi, 'MAY')
    .replaceAll(/\boptional\b/gi, 'OPTIONAL');

  out = out.charAt(0).toUpperCase() + out.slice(1);
  return out;
}
