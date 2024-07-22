import type { Config } from 'jest';

const esmModules = ['serialize-error'];

const config: Config = {
  preset: 'ts-jest/presets/js-with-ts',
  testEnvironment: 'node',
  collectCoverage: true,
  coveragePathIgnorePatterns: ['/node_modules/', '<rootDir>/__utils__/'],
  testMatch: ['**/?(*.)+(spec|test).[jt]s?(x)'],
  prettierPath: null,
  transformIgnorePatterns: [`node_modules/(?!(?:.pnpm/)?(${esmModules.join('|')}))`],
};

export default config;
