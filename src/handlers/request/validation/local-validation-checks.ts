type TemplateField = {
  id?: string;
  attributes?: { label?: string };
  validations?: { required?: boolean };
  [k: string]: unknown;
};

type TemplateLike = {
  body?: TemplateField[];
  [k: string]: unknown;
};

type FormData = Record<string, string>;

function escapeRegExpLocal(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function issueBodyHasTemplateFieldSection(issueBody: unknown, field: TemplateField): boolean {
  const body = String(issueBody || '');
  if (!body) return false;

  const candidates = [String(field?.attributes?.label || ''), String(field?.id || '')].filter(Boolean);

  return candidates.some((candidate) => {
    const re = new RegExp(`^###\\s+${escapeRegExpLocal(candidate)}\\s*$`, 'mi');
    return re.test(body);
  });
}

function issueBodyHasAnyIssueFormSection(issueBody: unknown): boolean {
  return /^###\s+\S+/m.test(String(issueBody || ''));
}

function shouldEnforceRequiredTemplateField(
  issueBody: unknown,
  field: TemplateField,
  formData: FormData,
  isEmpty: (value: unknown) => boolean
): boolean {
  const id = String(field?.id || '');
  if (!id) return false;

  if (!isEmpty((formData as Record<string, unknown>)[id])) {
    return false;
  }

  const hasAnyIssueFormSection = issueBodyHasAnyIssueFormSection(issueBody);

  // Unit tests / synthetic validation bodies often do not contain issue-form markdown sections.
  // In that case this is not a legacy issue-form body, so required validation must stay strict.
  if (!hasAnyIssueFormSection) {
    return true;
  }

  // Backwards compatibility:
  // If the issue body already has issue-form sections, but this specific section is missing,
  // treat it as an older issue created before the field was added.
  return issueBodyHasTemplateFieldSection(issueBody, field);
}

export function applyRequiredFieldValidation(args: {
  template: TemplateLike;
  formData: FormData;
  issueBody: unknown;
  formBucket: string[];
  errors: string[];
  isEmpty: (value: unknown) => boolean;
}): void {
  const requiredFields = (args.template?.body || []).filter((field) => field?.id && field.validations?.required);

  const missingRequired = requiredFields
    .filter((field) => shouldEnforceRequiredTemplateField(args.issueBody, field, args.formData, args.isEmpty))
    .map((field) => String(field?.attributes?.label || field.id));

  for (const label of missingRequired) {
    const msg = `Required field is missing in form: ${label}`;
    args.formBucket.push(msg);
    args.errors.push(msg);
  }
}

export function applySchemaIdentifierConsistencyCheck(args: {
  template: TemplateLike;
  schemaObj: unknown;
  schemaBucket: string[];
  errors: string[];
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
  getObjectProp: (obj: unknown, key: string) => Record<string, unknown> | null;
}): void {
  const schemaProps = args.getObjectProp(args.schemaObj, 'properties') || {};

  const idPropEntry = Object.entries(schemaProps).find(
    ([, def]) => args.isPlainObject(def) && def['x-form-field'] === 'identifier'
  );

  const hasIdentifierFieldInTemplate = Array.isArray(args.template?.body)
    ? args.template.body.some((field) => field?.id === 'identifier')
    : false;

  if (idPropEntry && !hasIdentifierFieldInTemplate) {
    const msg =
      'Configuration error: schema marks a primary identifier with x-form-field="identifier", but the form template has no field with id "identifier".';
    args.schemaBucket.push(msg);
    args.errors.push(msg);
  }
}
