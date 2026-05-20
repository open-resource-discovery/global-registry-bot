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

export function applyRequiredFieldValidation(args: {
  template: TemplateLike;
  formData: FormData;
  formBucket: string[];
  errors: string[];
  isEmpty: (value: unknown) => boolean;
}): void {
  const requiredFields = (args.template?.body || []).filter((field) => field?.id && field.validations?.required);

  const missingRequired = requiredFields
    .filter((field) => args.isEmpty((args.formData as Record<string, unknown>)?.[String(field.id)]))
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
