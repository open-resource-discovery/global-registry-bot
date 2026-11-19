/** @type {import('jest').Config} */
module.exports = {
  testEnvironment: 'node',

  // Tests
  testMatch: ['<rootDir>/test/**/*.test.ts'],

  // ESM + TS
  extensionsToTreatAsEsm: ['.ts'],
  transform: {
    '^.+\\.tsx?$': [
      'ts-jest',
      {
        useESM: true,
        tsconfig: '<rootDir>/tsconfig.test.json',
        diagnostics: {
          ignoreCodes: [2823, 1343],
        },
      },
    ],
  },

  moduleNameMapper: {
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },

  // Setup
  setupFilesAfterEnv: ['<rootDir>/test/jest.setup.ts'],

  // Coverage
  collectCoverageFrom: ['<rootDir>/src/**/*.ts'],
  coveragePathIgnorePatterns: ['/node_modules/', '/dist/'],

  coverageThreshold: {
    global: {
      branches: 65,
      functions: 90,
      lines: 90,
      statements: 80,
    },
  },
};
