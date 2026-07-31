import js from '@eslint/js';
import eslintConfigPrettier from 'eslint-config-prettier';
import globals from 'globals';
import tseslint from 'typescript-eslint';

/**
 * Shared ESLint configuration for non-React TypeScript packages: the API
 * service and the shared libraries.
 *
 * Deliberately does not use eslint-plugin-only-warn. The `base` config
 * downgrades every rule to a warning, which means lint can never fail a build.
 * These packages include the service that handles authentication, so their
 * findings must be able to gate CI.
 *
 * @type {import("eslint").Linter.Config[]}
 */
export const config = [
  js.configs.recommended,
  ...tseslint.configs.recommended,
  eslintConfigPrettier,
  {
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'module',
      globals: { ...globals.node },
    },
    rules: {
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          // Caught errors are a separate category from ordinary variables, and
          // an intentionally ignored one is a normal pattern.
          caughtErrorsIgnorePattern: '^_',
        },
      ],
      eqeqeq: ['error', 'smart'],
      'no-console': ['warn', { allow: ['warn', 'error', 'info'] }],
    },
  },
  {
    ignores: ['dist/**', 'build/**', 'node_modules/**'],
  },
];
