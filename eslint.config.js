import { withCustomConfig } from '@sap/eslint-config';

export default withCustomConfig([
  {
    ignores: ['dist', 'reports', 'node_modules', 'types/**/*.d.ts'],
  },
  {
    files: ['src/**/*.[jt]s'],
    languageOptions: {
      parserOptions: {
        project: 'tsconfig.json',
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      '@typescript-eslint/ban-ts-ignore': 'off',
      '@typescript-eslint/ban-ts-comment': 'off',
    },
  },
  {
    files: ['test/**/*.ts'],
    languageOptions: {
      parserOptions: {
        project: 'tsconfig.test.json',
        tsconfigRootDir: import.meta.dirname,
      },
    },
  },
]);
