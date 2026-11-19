import { describe, it, expect } from '@jest/globals';
import {
  applyRequiredFieldValidation,
  applySchemaIdentifierConsistencyCheck,
} from '../src/handlers/request/validation/local-validation-checks.js';

describe('applyRequiredFieldValidation — branch coverage', () => {
  it('L32 arm1: null issueBody → String(null || "") → issueBodyHasAnyIssueFormSection returns false → required enforced', () => {
    const formBucket: string[] = [];
    const errors: string[] = [];
    applyRequiredFieldValidation({
      template: { body: [{ id: 'field1', validations: { required: true }, attributes: { label: 'Field One' } }] },
      formData: { field1: '' },
      issueBody: null,
      formBucket,
      errors,
      isEmpty: (v: unknown) => !v,
    });
    expect(errors).toContain('Required field is missing in form: Field One');
  });

  it('L23 arm1 (label fallback): field with id but no attributes.label → label || "" uses "" in issueBodyHasTemplateFieldSection', () => {
    const formBucket: string[] = [];
    const errors: string[] = [];
    // issueBody has issue-form sections (### SomeOtherSection) → hasAnyIssueFormSection=true
    // field has id but no label → in issueBodyHasTemplateFieldSection, label || '' fires
    // The body doesn't contain ### myfield → section not found → field not enforced
    applyRequiredFieldValidation({
      template: { body: [{ id: 'myfield', validations: { required: true } }] },
      formData: { myfield: '' },
      issueBody: '### SomeOtherSection\nsome content here',
      formBucket,
      errors,
      isEmpty: (v: unknown) => !v,
    });
    // Field not enforced (issueBodyHasTemplateFieldSection returns false)
    expect(errors).toHaveLength(0);
  });
});

describe('applySchemaIdentifierConsistencyCheck — branch coverage', () => {
  it('L91 arm1: getObjectProp returns null → || {} fallback fires → schemaProps = {}', () => {
    const schemaBucket: string[] = [];
    const errors: string[] = [];
    applySchemaIdentifierConsistencyCheck({
      template: { body: [{ id: 'identifier' }] },
      schemaObj: { properties: null },
      schemaBucket,
      errors,
      isPlainObject: (v: unknown): v is Record<string, unknown> =>
        v !== null && typeof v === 'object' && !Array.isArray(v),
      getObjectProp: (_obj: unknown, _key: string): Record<string, unknown> | null => null,
    });
    // schemaProps = {} (no entries) → idPropEntry = undefined → no error
    expect(errors).toHaveLength(0);
  });

  it('triggers consistency error when x-form-field marks identifier but template has no identifier field', () => {
    const schemaBucket: string[] = [];
    const errors: string[] = [];
    applySchemaIdentifierConsistencyCheck({
      template: { body: [{ id: 'namespace' }] },
      schemaObj: {},
      schemaBucket,
      errors,
      isPlainObject: (v: unknown): v is Record<string, unknown> =>
        v !== null && typeof v === 'object' && !Array.isArray(v),
      getObjectProp: (_obj: unknown, _key: string): Record<string, unknown> | null => ({
        myProp: { 'x-form-field': 'identifier' },
      }),
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain('x-form-field');
  });
});
